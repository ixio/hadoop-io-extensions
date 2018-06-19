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


import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import javax.sound.sampled.AudioFileFormat;
import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.UnsupportedAudioFileException;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * A reader to read fixed length portions of a WAV file from a split.
 * It checks parameters validity against file header before starting,
 * skip the file header (if needed), and finally reads chunks of wav
 * data and convert them to Array[Array[Double]] (1 array per signal
 * channel).
 *
 * @author Joseph Allemandou
 */
public class WavPcmRecordReader extends RecordReader<LongWritable, TwoDDoubleArrayWritable> {

    // Starting to read from sound data: 44 bytes after file start
    public static final long HEADER_SIZE_IN_BYTES = 44;

    private static final Log LOG
            = LogFactory.getLog(WavPcmRecordReader.class);

    public enum PartialLastRecordAction {
        FAIL, SKIP, FILL
    }

    // Original parameters
    private Path file;
    private float sampleRate;
    private int channels;
    private int sampleSizeInBits;
    private int recordSizeInFrames;
    private PartialLastRecordAction partialLastRecordAction;

    // Computed parameters
    private int sampleSizeInBytes;
    private double doubleOffset;
    private double doubleScale;
    private byte partialLastRecordZeroFill;


    // Number of records the full contains
    // Only used in case of compressed file,
    // to validate file integrity
    private int fileRecordNumber;
    private boolean isCompressed;
    private int recordRead;

    // Inner reader and local key/value to return
    private FixedLengthWithOffsetRecordReader innerReader;
    private LongWritable key;
    private TwoDDoubleArrayWritable value;

    public WavPcmRecordReader(
            float sampleRate,
            int channels,
            int sampleSizeInBits,
            int recordSizeInFrames,
            PartialLastRecordAction partialLastRecordAction
    ) throws IOException {

        // Original parameters
        this.sampleRate = sampleRate;
        this.channels = channels;
        this.sampleSizeInBits = sampleSizeInBits;
        this.recordSizeInFrames = recordSizeInFrames;
        this.partialLastRecordAction = partialLastRecordAction;

        // Computed parameters
        this.sampleSizeInBytes = this.sampleSizeInBits / 8;

        // Prepare double-conversion values for signal
        if (this.sampleSizeInBits > 8) {
            this.doubleOffset = 0.0;
            this.doubleScale = (double)(1 << (this.sampleSizeInBits - 1));
            // No offset - byte 0 will lead to double 0
            this.partialLastRecordZeroFill = (byte)0;
        }
        else {
            this.doubleOffset = -1.0;
            this.doubleScale =  0.5 * ((1 << this.sampleSizeInBits) - 1);
            // Byte goes from -128 to 127. Unsigned-midrange is MaxValue :)
            this.partialLastRecordZeroFill = Byte.MAX_VALUE;
        }

        // Instantiate inner record reader
        this.innerReader = new FixedLengthWithOffsetRecordReader(
                HEADER_SIZE_IN_BYTES,
                this.recordSizeInFrames * this.channels * this.sampleSizeInBytes,
                FixedLengthWithOffsetRecordReader.PartialLastRecordAction.valueOf(
                        this.partialLastRecordAction.toString()),
                this.partialLastRecordZeroFill,
                true // When reading WAVs, we want the record-key to be shifted by offset
                );

    }

    @Override
    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        this.file = split.getPath();
        this.recordRead = 0;
        validateAudioParameters(job);
        this.innerReader.initialize(job, split.getStart(), split.getLength(), this.file);
    }

    public void validateAudioParameters(Configuration job) throws IOException {

        // Get input stream from file
        CompressionCodec codec = new CompressionCodecFactory(job).getCodec(this.file);
        final FileSystem fs = this.file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(this.file);
        InputStream inputStream;

        if (null != codec) {
            Decompressor decompressor = CodecPool.getDecompressor(codec);
            inputStream = codec.createInputStream(fileIn, decompressor);
            this.isCompressed = true;
            LOG.info("Reading wav header from compressed input.");
        } else {
            inputStream = fileIn;
            this.isCompressed = false;
            LOG.info("Reading wav header from uncompressed input.");
        }

        //
        try {
            // Putting the input stream in BufferedInputStream since
            // audio file format retrieval needs stream marking.
            AudioFileFormat audioFileFormat = AudioSystem.getAudioFileFormat(
                    new BufferedInputStream(inputStream));
            AudioFormat audioFormat = audioFileFormat.getFormat();

            // Check audio parameters
            List<String> errors = new ArrayList<>();
            if (audioFileFormat.getType() != AudioFileFormat.Type.WAVE) {
                errors.add("Input audio file " + this.file.toString() +" is not wav.");
            }
            if (audioFormat.isBigEndian()) {
                errors.add("Input audio file " + this.file.toString() + " is not little-endian formatted.");
            }
            if (audioFormat.getEncoding() != AudioFormat.Encoding.PCM_SIGNED &&
                    audioFormat.getEncoding() != AudioFormat.Encoding.PCM_UNSIGNED) {
                errors.add("Input audio file " + this.file.toString() + " is not PCM formatted");
            }
            if (audioFormat.getSampleRate() != this.sampleRate) {
                errors.add("Input audio file " + this.file.toString() + " sample rate ("
                        + audioFormat.getSampleRate() + ") doesn't match configured one ("
                        + this.sampleRate + ")");
            }
            if (audioFormat.getChannels() != this.channels) {
                errors.add("Input audio file " + this.file.toString() + " number of channels ("
                        + audioFormat.getChannels() + ") doesn't match configured one ("
                        + this.channels + ")");
            }
            if (audioFormat.getSampleSizeInBits() != this.sampleSizeInBits) {
                errors.add("Input audio file " + this.file.toString() + " sample size in bits ("
                        + audioFormat.getSampleSizeInBits() + ") doesn't match configured one ("
                        + this.sampleSizeInBits + ")");
            }

            // Validate file length at the beginning if file is not compressed
            if (! isCompressed) {
                long lengthInFs = fs.getFileStatus(this.file).getLen();
                if (lengthInFs != audioFileFormat.getByteLength()) {
                    errors.add("Input audio file " + this.file.toString() + " length doesn't match "
                            + "computed one " + lengthInFs + ". It is probably corrupted.");
                }
            }
            // Compute expected number of records in case file is entirely read
            switch (partialLastRecordAction) {
                // Complete records + 1 if incomplete
                case FAIL:
                case FILL:
                    fileRecordNumber = (int) Math.ceil(
                            audioFileFormat.getFrameLength() / (double) this.recordSizeInFrames);
                    break;
                // Complete records only
                case SKIP:
                    fileRecordNumber = (int) Math.floor(
                            audioFileFormat.getFrameLength() / (double) this.recordSizeInFrames);
            }
            inputStream.close();
            fileIn.close();

            if (errors.size() > 0) {
                throw new IOException("Errors in settings:\n" + StringUtils.join(errors, "\n"));
            }

        } catch (UnsupportedAudioFileException e) {
            throw new IOException("Input file is not wav.", e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        this.innerReader.close();
        this.innerReader = null;
    }

    @Override
    public synchronized boolean nextKeyValue() throws IOException {
        // Initialize key and value if not yet done
        if (null == this.key) {
            this.key = new LongWritable();
        }
        if (null == this.value) {
            this.value = new TwoDDoubleArrayWritable();
        }

        // Check innerReader nextValue, and load locally after transformation
        if (this.innerReader.nextKeyValue()) {
            this.recordRead += 1;
            this.key = this.innerReader.getCurrentKey();
            byte[] recordBytes = this.innerReader.getCurrentValue().getBytes();
            if (recordBytes.length % this.channels != 0) {
                throw new IOException("Record length in bytes " + recordBytes.length
                        + " is not divisible by number of channels " + this.channels);
            }

            // Prepare variables
            DoubleWritable[][] recordDoubles =
                    new DoubleWritable[this.channels][this.recordSizeInFrames];
            long extractedLong;
            int f, c , b, readInt, idx = 0;

            // Loops through frames, channel then sample converting bytes into doubles
            for (f = 0; f < this.recordSizeInFrames; f++) {
                for (c = 0; c < this.channels; c++) {
                    extractedLong = 0L;
                    for (b = 0; b < this.sampleSizeInBytes; b++) {
                        if (b < this.sampleSizeInBytes - 1 || sampleSizeInBytes == 1) {
                            readInt = recordBytes[idx] & 0xFF;
                        } else {
                            readInt = recordBytes[idx];
                        }
                        extractedLong += readInt << (b * 8);
                        idx += 1;
                    }
                    recordDoubles[c][f] = new DoubleWritable(
                            this.doubleOffset + (double)extractedLong / this.doubleScale);
                }
            }
            this.value.set(recordDoubles);
            return true;
        }
        if (this.isCompressed) {
            // Check number of record read matches the expected one
            // Else throw exception for corrupted file
            if (this.recordRead != this.fileRecordNumber) {
                throw new IOException("Enf of file " + this.file.toString() + " reached before "
                        + "expected number of records " + this.fileRecordNumber);
            }
        }
        return false;
    }

    @Override
    public LongWritable getCurrentKey() {
        return this.key;
    }

    @Override
    public TwoDDoubleArrayWritable getCurrentValue() {
        return this.value;
    }

    @Override
    public synchronized float getProgress() throws IOException {
        return this.innerReader.getProgress();
    }


}
