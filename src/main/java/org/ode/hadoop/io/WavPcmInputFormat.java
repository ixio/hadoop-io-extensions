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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * WavPCMInputFormat is an input format used to read WAV files encoded using
 * the PCM format. It subclasses the [[FixedLengthWithOffsetInputFormat]] class.
 * The format expects settings to be provided through configuration properties,
 * and validate them against the file header at RecordReader initialization.
 * The data is then read by chunks of size defined by property, and converted
 * on the fly as doubles.
 * Multiple strategies are available in case the file size is not divisible by
 * the chosen record size (fail, skip or fill, with a specified fill value).
 * <br><br>
 * Properties to be set:
 * <li>
 *     <ul>WavPcmInputFormat.setSampleRate</ul>
 *     <ul>WavPcmInputFormat.setChannels</ul>
 *     <ul>WavPcmInputFormat.setSampleSizeInBits</ul>
 *     <ul>WavPcmInputFormat.setRecordSizeInFrames</ul>
 *     <ul>WavPcmInputFormat.setPartialLastRecordAction -- (default to fail)</ul>
 * </li>
 * <br><br>
 *
 * @see WavPcmRecordReader
 *
 * @author Joseph Allemandou
 */
public class WavPcmInputFormat extends FileInputFormat<LongWritable, TwoDDoubleArrayWritable> {

    public static final String SAMPLE_RATE =
            "wavpcminputformat.samplerate";

    public static final String CHANNELS =
            "wavpcminputformat.channels";

    public static final String SAMPLE_SIZE_IN_BITS =
            "wavpcminputformat.samplesizeinbits";

    public static final String RECORD_SIZE_IN_FRAMES =
            "wavpcminputformat.recordsizeinframes";

    public static final String PARTIAL_LAST_RECORD_ACTION =
            "wavpcminputformat.partiallastrecord.action";
    public static final String PARTIAL_LAST_RECORD_ACTION_FAIL = "fail";
    public static final String PARTIAL_LAST_RECORD_ACTION_SKIP = "skip";
    public static final String PARTIAL_LAST_RECORD_ACTION_FILL = "fill";

    /**
     * Set the expected sample rate of the wave file
     * @param conf configuration
     * @param sampleRate the expected sample rate
     */
    public static void setSampleRate(Configuration conf, float sampleRate) {
        conf.setFloat(SAMPLE_RATE, sampleRate);
    }

    /**
     * Get sample rate value
     * @param conf configuration
     * @return the sample rate, zero means none was set
     */
    public static float getSampleRate(Configuration conf) {
        return conf.getFloat(SAMPLE_RATE, 0.0f);
    }


    /**
     * Set the expected number of channels of the wave file
     * @param conf configuration
     * @param channels the expected number of channels
     */
    public static void setChannels(Configuration conf, int channels) {
        conf.setInt(CHANNELS, channels);
    }

    /**
     * Get the number of channels
     * @param conf configuration
     * @return the number of channels, zero means none was set
     */
    public static int getChannels(Configuration conf) {
        return conf.getInt(CHANNELS, 0);
    }


    /**
     * Set the expected sample size in bits of the wave file
     * @param conf configuration
     * @param sampleSizeInBits the expected sample size in bits
     */
    public static void setSampleSizeInBits(Configuration conf, int sampleSizeInBits) {
        conf.setInt(SAMPLE_SIZE_IN_BITS, sampleSizeInBits);
    }

    /**
     * Get sample size in bits value
     * @param conf configuration
     * @return the sample size in bits, zero means none was set
     */
    public static int getSampleSizeInBits(Configuration conf) {
        return conf.getInt(SAMPLE_SIZE_IN_BITS, 0);
    }

    /**
     * Set the size in frames to read as a single record
     * @param conf configuration
     * @param recordSizeInFrames the size in frames of a single record
     */
    public static void setRecordSizeInFrames(Configuration conf, int recordSizeInFrames) {
        conf.setInt(RECORD_SIZE_IN_FRAMES, recordSizeInFrames);
    }

    /**
     * Get the size in frames of a single record
     * @param conf configuration
     * @return the size in frames of a single record, zero means none was set
     */
    public static int getRecordSizeInFrames(Configuration conf) {
        return conf.getInt(RECORD_SIZE_IN_FRAMES, 0);
    }


    /**
     * Set the action to take in case of partial last record
     * @param conf configuration
     * @param partialLastRecordAction the action to take (can be fail, skip
     *                                or fill-with-0).
     */
    public static void setPartialLastRecordAction(
            Configuration conf, String partialLastRecordAction) throws IOException {
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

    @Override
    public RecordReader<LongWritable, TwoDDoubleArrayWritable>
    createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        float sampleRate = getSampleRate(context.getConfiguration());
        int channels = getChannels(context.getConfiguration());
        int sampleSizeInBits = getSampleSizeInBits(context.getConfiguration());
        int recordSizeInFrames = getRecordSizeInFrames(context.getConfiguration());
        String partialLastRecordAction = getPartialLastRecordAction(context.getConfiguration());

        List<String> errors = new ArrayList<>();

        String errorMessage = " is invalid. It should be set to a value greater than zero.";

        if (sampleRate <= 0.0f) {
            errors.add("Sample rate " + sampleRate + errorMessage);
        }
        if (channels <= 0) {
            errors.add("Channels " + channels + errorMessage);
        }
        if (sampleSizeInBits <= 0) {
            errors.add("Sample size in bits " + sampleSizeInBits + errorMessage);
        }
        if (recordSizeInFrames <= 0) {
            errors.add("Record size in frames " + recordSizeInFrames + errorMessage);
        }
        if (! Arrays.asList(
                PARTIAL_LAST_RECORD_ACTION_FAIL,
                PARTIAL_LAST_RECORD_ACTION_SKIP,
                PARTIAL_LAST_RECORD_ACTION_FILL).contains(partialLastRecordAction)) {
            errors.add("Partial last record action " + partialLastRecordAction
                    + "is not one of [fail, skip, fill].");
        }
        if (errors.size() > 0) {
            throw new IOException("Errors in settings:\n" + StringUtils.join(errors, "\n"));
        }

        return new WavPcmRecordReader(sampleRate, channels, sampleSizeInBits, recordSizeInFrames,
                WavPcmRecordReader.PartialLastRecordAction.valueOf(partialLastRecordAction.toUpperCase()));
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        final CompressionCodec codec =
                new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return (null == codec);
    }

}
