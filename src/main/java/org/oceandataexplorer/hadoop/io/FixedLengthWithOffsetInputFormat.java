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

package org.oceandataexplorer.hadoop.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * FixedLengthWithHeaderInputFormat is an input format used to read input files
 * which contain fixed length records, with the ability to start reading
 * files from an offset and not fail in case the last record is not exactly the
 * expected size. The content of a record need not be text, it can be arbitrary
 * binary data.
 * <br><br>
 * Properties to be set:
 * <ul>
 *     <li>FixedLengthWithOffsetInputFormat.setRecordLength(conf, recordLength); (mandatory)</li>
 *     <li>FixedLengthWithOffsetInputFormat.setOffsetSize(conf, offsetSize); (0 by default)</li>
 *     <li>FixedLengthWithOffsetInputFormat.setPartialLastRecordAction(conf, partialLastRecordAction);<br>
 *        (fail by default, can be skip of fill. If fill is chosen, then next property needs to be defined)</li>
 *     <li>FixedLengthWithOffsetInputFormat.setPartialLastRecordFillValue(conf, partialLastRecordFillValue);<br>
 *         (The byte value that should be used to fill partial last record if fill as chosen above)</li>
 *     <li>FixedLengthWithOffsetInputFormat.setShiftRecordKeyByOffset(conf, shiftRecordKeyByOffset);<br>
 *         (true if record keys are to be shifted by offset, false for them to start with beginning of file)</li>
 * </ul>
 *
 * @see FixedLengthWithOffsetRecordReader
 *
 * This file is a modified copy of
 *   [[org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat]]
 * by Joseph Allemandou
 */
public class FixedLengthWithOffsetInputFormat
        extends FileInputFormat<LongWritable, BytesWritable> {

    public static final String OFFSET_SIZE =
            "fixedlengthwithoffsetinputformat.offset.size";

    public static final String FIXED_RECORD_LENGTH =
            "fixedlengthwithoffsetinputformat.record.length";

    public static final String PARTIAL_LAST_RECORD_ACTION =
            "fixedlengthwithoffsetinputformat.partiallastrecord.action";
    public static final String PARTIAL_LAST_RECORD_ACTION_FAIL = "fail";
    public static final String PARTIAL_LAST_RECORD_ACTION_SKIP = "skip";
    public static final String PARTIAL_LAST_RECORD_ACTION_FILL = "fill";

    public static final String PARTIAL_LAST_RECORD_FILL_VALUE =
            "fixedlengthwithoffsetinputformat.partiallastrecord.fillvalue";

    public static final String SHIFT_RECORD_KEY_BY_OFFSET =
            "fixedlengthwithoffsetinputformat.recordkey.shiftbyoffset";

    /**
     * Set the size of the file header to skip
     * @param conf configuration
     * @param offsetSize the size of the offset
     */
    public static void setOffsetSize(Configuration conf, long offsetSize) {
        conf.setLong(OFFSET_SIZE, offsetSize);
    }

    /**
     * Get offset size value
     * @param conf configuration
     * @return the record length, zero means none was set
     */
    public static long getOffsetSize(Configuration conf) {
        return conf.getLong(OFFSET_SIZE, 0L);
    }

    /**
     * Set the length of each record
     * @param conf configuration
     * @param recordLength the length of a record
     */
    public static void setRecordLength(Configuration conf, int recordLength) {
        conf.setInt(FIXED_RECORD_LENGTH, recordLength);
    }

    /**
     * Get record length value
     * @param conf configuration
     * @return the record length, zero means none was set
     */
    public static int getRecordLength(Configuration conf) {
        return conf.getInt(FIXED_RECORD_LENGTH, 0);
    }

    /**
     * Set the action to take in case of partial last record
     * @param conf configuration
     * @param partialLastRecordAction the action to take (can be fail, skip
     *                                or fill - If fill is selected, partial
     *                                last record fill value must also be set).
     */
    public static void setPartialLastRecordAction(
            Configuration conf, String partialLastRecordAction) {
        conf.setStrings(PARTIAL_LAST_RECORD_ACTION, partialLastRecordAction);
    }

    /**
     * Get the action to take in case of partial last record
     * @param conf configuration
     * @return the action to take (fail, skip, fill), fail if not set.
     */
    public static String getPartialLastRecordAction(Configuration conf) {
        return conf.get(PARTIAL_LAST_RECORD_ACTION, PARTIAL_LAST_RECORD_ACTION_FAIL);
    }

    /**
     * Set the value to fill-in partial last record if action is fill
     * @param conf configuration
     * @param partialLastRecordFillValue The value to fill the partial last record.
     */
    public static void setPartialLastRecordFillValue(
            Configuration conf, byte partialLastRecordFillValue) {
        conf.setInt(PARTIAL_LAST_RECORD_FILL_VALUE, partialLastRecordFillValue);
    }

    /**
     * Get the value to fill-in partial last record if action is fill
     * @param conf configuration
     * @return the byte value to fill the missing end of the partial last record.
     *         zero if not set, but check is made for this property to be set when
     *         PARTIAL_LAST_RECORD_ACTION is fill.
     */
    public static byte getPartialLastRecordFillValue(Configuration conf) {
        return (byte)conf.getInt(PARTIAL_LAST_RECORD_FILL_VALUE, 0);
    }

    /**
     * Set the value to have record-key shifted by offset or start at beginning of file
     * @param conf configuration
     * @param shiftRecordKeyByOffset Whether to shift or not
     */
    public static void setShiftRecordKeyByOffset(
            Configuration conf, boolean shiftRecordKeyByOffset) {
        conf.setBoolean(SHIFT_RECORD_KEY_BY_OFFSET, shiftRecordKeyByOffset);
    }

    /**
     * Get the value to shift record-key by offset or not
     * @param conf configuration
     * @return true if record-key is shifted by offset (meaning it starts at 0)
     *         false if it starts at the beginning of file (meaning it starts at offset + 1)
     *         Default to true if unset.
     */
    public static boolean getShiftRecordKeyByOffset(Configuration conf) {
        return conf.getBoolean(SHIFT_RECORD_KEY_BY_OFFSET, true);
    }

    /**
     *
     * @param split The split read b the RecordReader
     * @param context The job context
     * @return a new RecordReader initialized with the given split and context
     * @throws IOException in case provided settings are invalid
     */
    @Override
    public RecordReader<LongWritable, BytesWritable>
    createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException {

        Configuration conf = context.getConfiguration();
        int recordLength = getRecordLength(conf);
        long offsetSize = getOffsetSize(conf);
        String partialLastRecordAction = getPartialLastRecordAction(conf);
        byte partialLastRecordFillValue = getPartialLastRecordFillValue(conf);
        boolean shiftRecordKeyByOffset = getShiftRecordKeyByOffset(conf);

        List<String> errors = new ArrayList<>();

        if (offsetSize < 0) {
            errors.add("Offset size " + offsetSize
                    + " is invalid.  It should be set to a value greater or equal than zero");
        }
        if (recordLength <= 0) {
            errors.add("Fixed record length " + recordLength
                    + " is invalid.  It should be set to a value greater than zero");
        }
        if (! Arrays.asList(
                PARTIAL_LAST_RECORD_ACTION_FAIL,
                PARTIAL_LAST_RECORD_ACTION_SKIP,
                PARTIAL_LAST_RECORD_ACTION_FILL).contains(partialLastRecordAction)) {
            errors.add("Partial last record action " + partialLastRecordAction
                    + "is not one of [fail, skip, fill].");
        }
        if ((PARTIAL_LAST_RECORD_ACTION_FILL.equals(partialLastRecordAction))
                && (null == context.getConfiguration().get(PARTIAL_LAST_RECORD_FILL_VALUE))){
            errors.add("Partial last record action is set to 'fill' but no fill value is define.");
        }
        if (errors.size() > 0) {
            throw new IOException("Errors in settings:\n" + StringUtils.join(errors, "\n"));
        }

        FixedLengthWithOffsetRecordReader.PartialLastRecordAction partialLastRecordActionEnum =
                FixedLengthWithOffsetRecordReader.PartialLastRecordAction.valueOf(
                        partialLastRecordAction.toUpperCase());

        return new FixedLengthWithOffsetRecordReader(
                offsetSize,
                recordLength,
                partialLastRecordActionEnum,
                partialLastRecordFillValue,
                shiftRecordKeyByOffset
        );
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        final CompressionCodec codec =
                new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return (null == codec);
    }

}
