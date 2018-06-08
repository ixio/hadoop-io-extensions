/**
 * Copyright 2018 OceanDataExplorer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ode.hadoop.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * A reader to read fixed length records from a split.
 * It offers the possibility to skip a header of at the beginning of files and
 * either fail, skip or fill in case a files ends over a partial record.
 *
 * Record offset is returned as key and the record as bytes is returned in value.
 *
 * This file is a modified copy of
 *   [[org.apache.hadoop.mapreduce.lib.input.FixedLengthRecordReader]]
 * by Joseph Allemandou
 */
public class FixedLengthWithOffsetRecordReader
        extends RecordReader<LongWritable, BytesWritable> {

    private static final Log LOG
            = LogFactory.getLog(FixedLengthWithOffsetRecordReader.class);

    public enum PartialLastRecordAction {
        FAIL, SKIP, FILL
    }

    private long offsetSize;
    private int recordLength;
    private PartialLastRecordAction partialLastRecordAction;
    private byte partialLastRecordFill;

    private long fileLength;
    private long start;
    private long pos;
    private long end;
    private long  numRecordsRemainingInSplit;
    private Seekable filePosition;
    private LongWritable key;
    private BytesWritable value;
    private boolean isCompressedInput;
    private Decompressor decompressor;
    private InputStream inputStream;

    public FixedLengthWithOffsetRecordReader(
            long offsetSize,
            int recordLength,
            PartialLastRecordAction partialLastRecordAction,
            byte partialLastRecordFill) {
        this.offsetSize = offsetSize;
        this.recordLength = recordLength;
        this.partialLastRecordAction = partialLastRecordAction;
        this.partialLastRecordFill = partialLastRecordFill;
    }

    public FixedLengthWithOffsetRecordReader(long offsetSize, int recordLength,
            PartialLastRecordAction partialLastRecordAction) throws IOException {
        this(offsetSize, recordLength, partialLastRecordAction, (byte)0);
        if (PartialLastRecordAction.FILL != partialLastRecordAction) {
            throw new IOException("partialLastRecordAction is not to be FILL " +
                    "when initializing without partialLastRecordFill.");
        }

    }

    public FixedLengthWithOffsetRecordReader(long offsetSize, int recordLength) {
        this(offsetSize, recordLength, PartialLastRecordAction.FAIL, (byte)0);
    }

    @Override
    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        final Path file = split.getPath();
        initialize(job, split.getStart(), split.getLength(), file);
    }

    // This is also called from the old FixedLengthRecordReader API implementation
    public void initialize(Configuration job, long splitStart, long splitLength,
                           Path file) throws IOException {
        start = splitStart;
        end = start + splitLength;
        CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);

        // Partial records can only be found in splits after offset
        long partialRecordLength = Math.max(start - offsetSize, 0L) % recordLength;

        // skip offset or whole split if start is before offset
        long numBytesToSkip = 0L;
        if (start < offsetSize) {
            if (null != codec) {
                // Non-splitable data - skip offsetSize directly
                numBytesToSkip = offsetSize;
            } else {
                // Splitable data - skip max splitLength
                numBytesToSkip = Math.min(offsetSize - start, splitLength);
            }
        // Start is after offset, check for partial record
        } else if (partialRecordLength != 0) {
            numBytesToSkip = recordLength - partialRecordLength;
        }

        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(job);
        fileLength = fs.getFileStatus(file).getLen();
        FSDataInputStream fileIn = fs.open(file);

        if (null != codec) {
            isCompressedInput = true;
            decompressor = CodecPool.getDecompressor(codec);
            CompressionInputStream cIn
                    = codec.createInputStream(fileIn, decompressor);
            filePosition = cIn;
            inputStream = cIn;
            numRecordsRemainingInSplit = Long.MAX_VALUE;
            LOG.info(
                    "Compressed input; cannot compute number of records in the split");
        } else {
            // Fail fast in case of expected partial last record failure
            if ((fileLength - offsetSize) % recordLength != 0
                    && PartialLastRecordAction.FAIL == partialLastRecordAction) {
                throw new IOException("File " + file.toString() + " contains a partial last record and "
                        + "PartialLastRecordAction is set to FAIL.");
            }
            fileIn.seek(start);
            filePosition = fileIn;
            inputStream = fileIn;
            long splitSize = end - start - numBytesToSkip;
            numRecordsRemainingInSplit = (splitSize + recordLength - 1)/recordLength;
            if (numRecordsRemainingInSplit < 0) {
                numRecordsRemainingInSplit = 0;
            }
            LOG.info("Expecting " + numRecordsRemainingInSplit
                    + " records each with a length of " + recordLength
                    + " bytes in the split with an effective size of "
                    + splitSize + " bytes");
        }
        if (numBytesToSkip != 0) {
            start += inputStream.skip(numBytesToSkip);
        }
        this.pos = start;
    }

    @Override
    public synchronized boolean nextKeyValue() throws IOException {
        // Initialise key and value if not yet done
        if (key == null) {
            key = new LongWritable();
        }
        if (value == null) {
            value = new BytesWritable(new byte[recordLength]);
        }

        // Read using a new byte array
        value.setSize(recordLength);
        boolean dataRead = false;
        byte[] record = value.getBytes();
        if (numRecordsRemainingInSplit > 0) {
            key.set(pos);
            int offset = 0;
            int numBytesToRead = recordLength;
            int numBytesRead; // Initialized at 0 by default
            while (numBytesToRead > 0) {
                numBytesRead = inputStream.read(record, offset, numBytesToRead);
                if (numBytesRead == -1) {
                    // EOF
                    break;
                }
                offset += numBytesRead;
                numBytesToRead -= numBytesRead;
            }
            numBytesRead = recordLength - numBytesToRead;
            pos += numBytesRead;
            if (numBytesRead > 0) {
                dataRead = true;
                if (numBytesRead >= recordLength) {
                    if (!isCompressedInput) {
                        numRecordsRemainingInSplit--;
                    }
                } else {
                    // Manage partial last record - Check we are at end of file
                    // for non-compressed files
                    if (isCompressedInput || pos == fileLength) {
                        switch (partialLastRecordAction) {
                            case FAIL:
                                throw new IOException("Partial last record found (length = " + numBytesRead
                                        +") and PartialLastRecordAction is set to FAIL.");
                            case SKIP:
                                LOG.info("Skipping partial last record (length = " + numBytesRead
                                        + ") as defined in configuration.");
                                numRecordsRemainingInSplit = 0L;
                                return false;
                            case FILL:
                                LOG.info("Filling partial last record (length = " + numBytesRead
                                        + ") with value " + partialLastRecordFill
                                        + " as defined in configuration.");
                                Arrays.fill(record, numBytesRead, numBytesRead + numBytesToRead, partialLastRecordFill);
                        }
                    } else {
                        // file is not compressed and current reading position is not end-of-file
                        throw new IOException("Unexpected partial record (length = " + numBytesRead + ")");
                    }

                }
            } else {
                numRecordsRemainingInSplit = 0L; // End of input.
            }
        }
        return dataRead;
    }

    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() {
        return value;
    }

    @Override
    public synchronized float getProgress() throws IOException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
        }
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            if (inputStream != null) {
                inputStream.close();
                inputStream = null;
            }
        } finally {
            if (decompressor != null) {
                CodecPool.returnDecompressor(decompressor);
                decompressor = null;
            }
        }
    }

    // This is called from the old FixedLengthRecordReader API implementation.
    public long getPos() {
        return pos;
    }

    private long getFilePosition() throws IOException {
        long retVal;
        if (isCompressedInput && null != filePosition) {
            retVal = filePosition.getPos();
        } else {
            retVal = pos;
        }
        return retVal;
    }

}
