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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ode.hadoop.util.MapReduceTestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This file test the WavPcmInputFormat, compressed and not.
 * @author Joseph Allemandou
 */
public class TestWavPcmInputFormat {

    private static final Logger LOG =
            LoggerFactory.getLogger(TestWavPcmInputFormat.class);


    private static Configuration defaultConf;

    @BeforeClass
    public static void onlyOnce() {
        defaultConf = new Configuration();
        defaultConf.set("fs.defaultFS", "file:///");
    }

    private String soundFilePath = "sin_16kHz_2.5s.wav";
    List<Double> expectedFirstWave = Arrays.asList(
            0.0d,
            0.3826904296875d,
            0.707122802734375d,
            0.923858642578125d,
            0.999969482421875d,
            0.92388916015625d,
            0.707061767578125d,
            0.382781982421875d,
            -9.1552734375E-5d,
            -0.38262939453125d,
            -0.707122802734375d,
            -0.923919677734375d,
            -0.99993896484375d,
            -0.9239501953125d,
            -0.70703125d,
            -0.382720947265625d);

    private String corruptedSoundFilePath = "sin_16kHz_2.5s_corrupted.wav";


    @Test
    public void testFailNoConfigParameter() throws URISyntaxException, IOException, InterruptedException {
        URI soundUri = getClass().getClassLoader().getResource(soundFilePath).toURI();
        Path file = new Path(soundUri);
        // Create the job and do not set fixed record length
        Job job = Job.getInstance(defaultConf);
        FileInputFormat.setInputPaths(job, file);
        WavPcmInputFormat format = new WavPcmInputFormat();
        List<InputSplit> splits = format.getSplits(job);
        boolean exceptionThrown = false;
        for (InputSplit split : splits) {
            try {
                TaskAttemptContext context = MapReduceTestUtil.
                        createDummyMapTaskAttemptContext(job.getConfiguration());
                RecordReader<LongWritable, TwoDDoubleArrayWritable> reader =
                        format.createRecordReader(split, context);
                MapContext<LongWritable, TwoDDoubleArrayWritable, LongWritable, TwoDDoubleArrayWritable>
                        mcontext = new MapContextImpl<>(
                                job.getConfiguration(), context.getTaskAttemptID(),
                                reader, null, null, MapReduceTestUtil.createDummyReporter(), split);
                reader.initialize(split, mcontext);
            } catch(IOException ioe) {
                exceptionThrown = true;
            }
        }
        assertTrue("Exception for not setting required configuration parameters: ", exceptionThrown);

    }

    @Test
    public void testReadSingleSplit() throws Exception {
        testRead(null, 1024*1024, 1000, null);
        testRead(null, 1024*1024, 300, "skip");
        testRead(null, 1024*1024, 300, "fill");
    }

    @Test
    public void testReadMultipleSplit() throws Exception {
        for (int i = 2; i < 5; i++) {
            testRead(null, (int)(2.5 * 16000 * 2) / i, 1000, null);
            testRead(null, (int)(2.5 * 16000 * 2) / i, 300, "skip");
            testRead(null, (int)(2.5 * 16000 * 2) / i, 300, "fill");
        }

    }

    @Test
    public void testReadSingleSplitCompressed() throws Exception {
        CompressionCodec gzip = new GzipCodec();
        testRead(gzip, 1024*1024, 1000, null);
        testRead(gzip, 1024*1024, 300, "skip");
        testRead(gzip, 1024*1024, 300, "fill");
    }

    @Test
    public void testReadMultipleSplitCompressed() throws Exception {
        CompressionCodec gzip = new GzipCodec();
        for (int i = 2; i < 5; i++) {
            testRead(gzip, (int)(2.5 * 16000 * 2) / (i * 5), 1000, null);
            testRead(gzip, (int)(2.5 * 16000 * 2) / (i * 5), 300, "skip");
            testRead(gzip, (int)(2.5 * 16000 * 2) / (i * 5), 300, "fill");
        }
    }

    @Test
    public void testFailReadSingleSplitLastPartial() throws Exception {
        boolean exceptionThrown = false;
        try {
            testRead(null, 1024 * 1024, 3000, null);
        } catch (IOException ioe) {
            exceptionThrown = true;
            LOG.info("Exception message:" + ioe.getMessage());
        }
        assertTrue("Exception for partial last record:", exceptionThrown);
    }

    private void testRead(
            CompressionCodec codec,
            int maxSplit,
            int recordSizeInFrame,
            String partialLastRecordAction) throws Exception {
        String soundPath = soundFilePath;
        if (null != codec) {
            soundPath += ".gz";
        }
        URI soundUri = getClass().getClassLoader().getResource(soundPath).toURI();
        Path file = new Path(soundUri);
        // Create the job and do not set fixed record length
        Job job = Job.getInstance(defaultConf);
        if (codec != null) {
            ReflectionUtils.setConf(codec, job.getConfiguration());
        }
        LongWritable key;
        TwoDDoubleArrayWritable value;
        int recordNumber = 0;
        List<DoubleWritable[][]> records = new ArrayList<>();

        WavPcmInputFormat.setSampleRate(job.getConfiguration(), 16000.0f);
        WavPcmInputFormat.setChannels(job.getConfiguration(), 1);
        WavPcmInputFormat.setSampleSizeInBits(job.getConfiguration(), 16);
        WavPcmInputFormat.setRecordSizeInFrames(job.getConfiguration(), recordSizeInFrame);
        if (null != partialLastRecordAction) {
            WavPcmInputFormat.setPartialLastRecordAction(job.getConfiguration(), partialLastRecordAction);
        }
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", maxSplit);

        FileInputFormat.setInputPaths(job, file);
        WavPcmInputFormat format = new WavPcmInputFormat();
        List<InputSplit> splits = format.getSplits(job);
        for (InputSplit split : splits) {
            TaskAttemptContext context = MapReduceTestUtil.
                    createDummyMapTaskAttemptContext(job.getConfiguration());
            RecordReader<LongWritable, TwoDDoubleArrayWritable> reader =
                    format.createRecordReader(split, context);
            MapContext<LongWritable, TwoDDoubleArrayWritable, LongWritable, TwoDDoubleArrayWritable>
                    mcontext = new MapContextImpl<>(
                    job.getConfiguration(), context.getTaskAttemptID(),
                    reader, null, null, MapReduceTestUtil.createDummyReporter(), split);

            reader.initialize(split, mcontext);

            Class<?> clazz = reader.getClass();
            assertEquals("RecordReader class should be WavPcmRecordReader:",
                    WavPcmRecordReader.class, clazz);
            // Plow through the records in this split
            while (reader.nextKeyValue()) {
                key = reader.getCurrentKey();
                value = reader.getCurrentValue();

                assertEquals("Checking key", (long)(44 + recordNumber * recordSizeInFrame * 2),
                        key.get());
                DoubleWritable[][] valueArray = (DoubleWritable[][])value.get();
                records.add(valueArray);
                assertEquals("Checking record channels dim:", 1, valueArray.length);
                assertEquals("Checking record signal dim:", recordSizeInFrame, valueArray[0].length);
                if (key.get() == 0) {
                    for (int i = 0; i < 16; i++) {
                        assertEquals("Checking first values of first record:",
                                expectedFirstWave.get(i).doubleValue(), valueArray[0][i].get(), 10e-15);
                    }
                }
                recordNumber++;
            }
            reader.close();
        }
        // Tests assume we fill at least 10 values with 0 in last record
        if ("fill".equals(partialLastRecordAction)) {
            assertEquals("Total original records should be total read records:",
                    (int) Math.ceil(2.5 * 16000 / recordSizeInFrame), recordNumber);
            DoubleWritable[] partialRecordSignal = records.get(recordNumber - 1)[0];
            for (int v = partialRecordSignal.length - 10; v < partialRecordSignal.length; v++) {
                assertEquals("Checking last value of partial signal to be 0",
                        partialRecordSignal[v].get(), 0.0d, 10e-15);
            }
        } else {
            assertEquals("Total original records should be total read records:",
                    (int)(2.5 * 16000 / recordSizeInFrame), recordNumber);
        }

    }


    @Test
    public void testReadSingleSplitCorrupted() throws Exception {
        testReadCorrupted(null, 1024*1024);
    }

    @Test
    public void testReadMultipleSplitCorrupted() throws Exception {
        for (int i = 2; i < 5; i++) {
            testReadCorrupted(null, (int)(2.5 * 16000 * 2) / i );
        }

    }

    @Test
    public void testReadSingleSplitCompressedCorrupted() throws Exception {
        CompressionCodec gzip = new GzipCodec();
        testReadCorrupted(gzip, 1024*1024);
    }

    @Test
    public void testReadMultipleSplitCompressedCorrupted() throws Exception {
        CompressionCodec gzip = new GzipCodec();
        for (int i = 2; i < 5; i++) {
            testReadCorrupted(gzip, (int)(2.5 * 16000 * 2) / (i * 5));
        }

    }


    private void testReadCorrupted(CompressionCodec codec, int maxSplit) throws Exception {
        String soundPath = corruptedSoundFilePath;
        if (null != codec) {
            soundPath += ".gz";
        }
        URI soundUri = getClass().getClassLoader().getResource(soundPath).toURI();
        Path file = new Path(soundUri);
        // Create the job and do not set fixed record length
        Job job = Job.getInstance(defaultConf);
        if (codec != null) {
            ReflectionUtils.setConf(codec, job.getConfiguration());
        }
        LongWritable key;
        TwoDDoubleArrayWritable value;
        int recordNumber = 0;

        WavPcmInputFormat.setSampleRate(job.getConfiguration(), 16000.0f);
        WavPcmInputFormat.setChannels(job.getConfiguration(), 1);
        WavPcmInputFormat.setSampleSizeInBits(job.getConfiguration(), 16);
        WavPcmInputFormat.setRecordSizeInFrames(job.getConfiguration(), 1000);
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", maxSplit);

        FileInputFormat.setInputPaths(job, file);
        WavPcmInputFormat format = new WavPcmInputFormat();
        List<InputSplit> splits = format.getSplits(job);
        boolean exceptionThrown = false;
        try {
            for (InputSplit split : splits) {
                TaskAttemptContext context = MapReduceTestUtil.
                        createDummyMapTaskAttemptContext(job.getConfiguration());
                RecordReader<LongWritable, TwoDDoubleArrayWritable> reader =
                        format.createRecordReader(split, context);
                MapContext<LongWritable, TwoDDoubleArrayWritable, LongWritable, TwoDDoubleArrayWritable>
                        mcontext = new MapContextImpl<>(
                        job.getConfiguration(), context.getTaskAttemptID(),
                        reader, null, null, MapReduceTestUtil.createDummyReporter(), split);

                reader.initialize(split, mcontext);

                Class<?> clazz = reader.getClass();
                assertEquals("RecordReader class should be WavPcmRecordReader:",
                        WavPcmRecordReader.class, clazz);
                // Plow through the records in this split
                while (reader.nextKeyValue()) {
                    key = reader.getCurrentKey();
                    value = reader.getCurrentValue();

                    assertEquals("Checking key", (long) (44 + recordNumber * 1000 * 2),
                            key.get());
                    DoubleWritable[][] valueArray = (DoubleWritable[][]) value.get();
                    assertEquals("Checking record channels dim:", 1, valueArray.length);
                    assertEquals("Checking record signal dim:", 1000, valueArray[0].length);
                    if (key.get() == 0) {
                        for (int i = 0; i < 16; i++) {
                            assertEquals("Checking first values of first record:",
                                    expectedFirstWave.get(i).doubleValue(), valueArray[0][i].get(), 0.00001);
                        }
                    }
                    recordNumber++;
                }
                reader.close();
            }
        } catch(IOException ioe) {
            exceptionThrown = true;
        }
        assertTrue("Exception trying to read corrupted file: ", exceptionThrown);
    }

}
