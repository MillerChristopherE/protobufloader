package org.protobufloader.util;

import org.protobufloader.util.Base64;

import java.io.*;
import java.util.*;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;


public abstract class ProtoInputFormat
        extends org.apache.hadoop.mapreduce.lib.input.FileInputFormat<LongWritable, BytesWritable>
{
    protected boolean allowEntireFileFail;
    
    public ProtoInputFormat(boolean allowEntireFileFail)
    {
        this.allowEntireFileFail = allowEntireFileFail;
    }
    
    public ProtoInputFormat()
    {
        this(false);
    }
    
    /*
    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split,
        org.apache.hadoop.mapreduce.TaskAttemptContext context);
    */
    
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec = 
            new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }
    
    @Override
    protected List<org.apache.hadoop.fs.FileStatus> listStatus(JobContext job) throws IOException
    {
        return getAllFileRecursively(super.listStatus(job), job.getConfiguration());
    }
    
    
    // Input formats:
    
    // Any supported based on runtime conditions.
    public static class AnyInput extends ProtoInputFormat
    {
        public AnyInput(boolean allowEntireFileFail)
        {
            super(allowEntireFileFail);
        }
        
        public AnyInput()
        {
            this(false);
        }
        
        @Override
        public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split,
            org.apache.hadoop.mapreduce.TaskAttemptContext context)
        {
            boolean isbin = false;
            Configuration job = context.getConfiguration();
            // First check the property.
            String propfmt = job.get("protobufloader.format"); // bin or line
            if(propfmt != null)
            {
                if("bin".equals(propfmt))
                {
                    isbin = true;
                }
            }
            else
            {
                // Detect based on extension.
                if(split instanceof FileSplit)
                {
                    FileSplit fsplit = (FileSplit)split;
                    String inputPath = fsplit.getPath().toString();
                    if(inputPath.endsWith(".bin")
                        || inputPath.endsWith(".bin.lzo")
                        || inputPath.endsWith(".bin.gz")
                        )
                    {
                        isbin = true;
                    }
                }
            }
            if(isbin)
            {
                System.out.println("AnyInput: using RecordReaderBin");
                return new RecordReaderBin(allowEntireFileFail);
            }
            System.out.println("AnyInput: using RecordReaderLine");
            return new RecordReaderLine(allowEntireFileFail);
        }
    }
    
    public static class BinInput extends ProtoInputFormat
    {
        public BinInput(boolean allowEntireFileFail)
        {
            super(allowEntireFileFail);
        }
        
        public BinInput()
        {
            this(false);
        }
        
        @Override
        public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split,
            org.apache.hadoop.mapreduce.TaskAttemptContext context)
        {
            return new RecordReaderBin(allowEntireFileFail);
        }
    }
    
    public static class LineInput extends ProtoInputFormat
    {
        public LineInput(boolean allowEntireFileFail)
        {
            super(allowEntireFileFail);
        }
        
        public LineInput()
        {
            this(false);
        }
        
        @Override
        public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split,
            org.apache.hadoop.mapreduce.TaskAttemptContext context)
        {
            return new RecordReaderLine(allowEntireFileFail);
        }
    }
    
    
    // Record readers:
    
    public static class RecordReaderBin
        extends org.apache.hadoop.mapreduce.RecordReader<LongWritable, BytesWritable>
    {
        int failstate = 0;
        long recnum = 0;
        LongWritable key = new LongWritable(0);
        BytesWritable value = new BytesWritable(new byte[1024]);
        CompressionCodecFactory compressionCodecs = null;
        DataInputStream in;
        long cur, end;
        
        public RecordReaderBin(boolean allowEntireFileFail)
        {
            if(allowEntireFileFail)
            {
                failstate = 1;
            }
        }
        
        public RecordReaderBin()
        {
            this(false);
        }
        
        @Override
        public void initialize(InputSplit genericSplit,
                org.apache.hadoop.mapreduce.TaskAttemptContext context) throws IOException
        {
            try
            {
                FileSplit split = (FileSplit)genericSplit;
                if(split.getStart() != 0)
                {
                    throw new RuntimeException("ProtoRecordReaderBin cannot read from split input");
                }
                cur = 0;
                end = split.getLength();
                Configuration job = context.getConfiguration();
                final Path file = split.getPath();
                compressionCodecs = new CompressionCodecFactory(job);
                final CompressionCodec codec = compressionCodecs.getCodec(file);
                
                org.apache.hadoop.fs.FileSystem fs = file.getFileSystem(job);
                FSDataInputStream fileIn = fs.open(split.getPath());
                in = fileIn;
                if(codec != null)
                {
                    in = new DataInputStream(codec.createInputStream(fileIn));
                }
            }
            catch(IOException e)
            {
                if(failstate == 0)
                {
                    throw e;
                }
                failstate = 2;
            }
        }
        
        @Override
        public boolean nextKeyValue() throws IOException
        {
            if(failstate == 2)
            {
                System.out.println("IOException:ENTIRE_FILE_FAIL");
                /*
                PigStatusReporter rep = PigStatusReporter.getInstance();
                rep.getCounter("ProtobufLoadFields", "IOException:_total").increment(1);
                rep.getCounter("ProtobufLoadFields", "IOException:ENTIRE_FILE_FAIL").increment(1);
                */
                return false;
            }
            try
            {
                value.readFields(in);
                key.set(++recnum);
                return true;
            }
            catch(EOFException eof)
            {
                return false;
            }
        }
        
        @Override
        public LongWritable getCurrentKey()
        {
            return key;
        }
        
        @Override
        public BytesWritable getCurrentValue()
        {
            return value;
        }
        
        public float getProgress()
        {
            if(cur == 0)
            {
                return 0;
            }
            return Math.min(1.0f, cur / end); // From entire split.
        }
        
        public synchronized void close() throws IOException
        {
            if(in != null)
            {
                in.close();
            }
        }
    }
    
    public static class RecordReaderLine
        extends org.apache.hadoop.mapreduce.RecordReader<LongWritable, BytesWritable>
    {
        int failstate = 0;
        org.apache.hadoop.mapreduce.lib.input.LineRecordReader input;
        BytesWritable value;
        
        public RecordReaderLine(boolean allowEntireFileFail)
        {
            if(allowEntireFileFail)
            {
                failstate = 1;
            }
            input = new org.apache.hadoop.mapreduce.lib.input.LineRecordReader();
        }
        
        public RecordReaderLine()
        {
            this(false);
        }
        
        @Override
        public void initialize(InputSplit genericSplit,
                org.apache.hadoop.mapreduce.TaskAttemptContext context) throws IOException
        {
            try
            {
                FileSplit split = (FileSplit)genericSplit;
                if(split.getStart() != 0)
                {
                    System.out.println("ProtoRecordReaderLine is reading from split input");
                }
                input.initialize(genericSplit, context);
            }
            catch(IOException e)
            {
                if(failstate == 0)
                {
                    throw e;
                }
                failstate = 2;
            }
        }
        
        @Override
        public boolean nextKeyValue() throws IOException
        {
            if(failstate == 2)
            {
                System.out.println("IOException:ENTIRE_FILE_FAIL");
                /*
                PigStatusReporter rep = PigStatusReporter.getInstance();
                rep.getCounter("ProtobufLoadFields", "IOException:_total").increment(1);
                rep.getCounter("ProtobufLoadFields", "IOException:ENTIRE_FILE_FAIL").increment(1);
                */
                return false;
            }
            if(input.nextKeyValue())
            {
                Text line = input.getCurrentValue();
                byte[] pbraw = Base64.decodeBase64(line.getBytes(), line.getLength());
                value = new BytesWritable(pbraw);
                return true;
            }
            return false;
        }
        
        @Override
        public LongWritable getCurrentKey()
        {
            return input.getCurrentKey();
        }
        
        @Override
        public BytesWritable getCurrentValue()
        {
            return value;
        }
        
        public float getProgress()
        {
            return input.getProgress();
        }
        
        public void close() throws IOException
        {
            input.close();
        }
    }
    
    
    // Other helpers:
    
    // The Apache Software Foundation.
    public static List<FileStatus> getAllFileRecursively(
            List<FileStatus> files, Configuration conf) throws IOException {
        List<FileStatus> result = new ArrayList<FileStatus>();
        int len = files.size();
        for (int i = 0; i < len; ++i) {
            FileStatus file = files.get(i);
            if (file.isDir()) {
                Path p = file.getPath();
                org.apache.hadoop.fs.FileSystem fs = p.getFileSystem(conf);
                addInputPathRecursively(result, fs, p, hiddenFileFilter);
            } else {
                result.add(file);
            }
        }
        System.out.println("Total input paths to process : " + result.size()); 
        return result;
    }
    
    // The Apache Software Foundation.
    private static void addInputPathRecursively(List<FileStatus> result,
            org.apache.hadoop.fs.FileSystem fs, Path path, PathFilter inputFilter) 
            throws IOException {
        for (FileStatus stat: fs.listStatus(path, inputFilter)) {
            if (stat.isDir()) {
                addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
            } else {
                result.add(stat);
            }
        }
    }

    // The Apache Software Foundation.
    private static final PathFilter hiddenFileFilter = new PathFilter(){
        public boolean accept(Path p){
            String name = p.getName();
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };
    
    
}

