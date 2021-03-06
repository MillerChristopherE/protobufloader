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
    protected int allowInputFailure = -1;
    
    public ProtoInputFormat(int allowInputFailure)
    {
        this.allowInputFailure = allowInputFailure;
    }
        
    public ProtoInputFormat(boolean allowEntireFileFail)
    {
        this(allowEntireFileFail ? Short.MAX_VALUE : 0);
    }
    
    public ProtoInputFormat()
    {
        this(-1);
    }
    
    protected void ensureSetup(org.apache.hadoop.mapreduce.TaskAttemptContext context)
    {
        Configuration job = context.getConfiguration();
        if(allowInputFailure == -1)
        {
            allowInputFailure = job.getInt("protobufloader.input.failures.allow", 0);
        }
    }
    
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
        public AnyInput(int allowInputFailure)
        {
            super(allowInputFailure);
        }
        
        public AnyInput(boolean allowEntireFileFail)
        {
            super(allowEntireFileFail);
        }
        
        public AnyInput()
        {
            super(-1);
        }
        
        @Override
        public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split,
            org.apache.hadoop.mapreduce.TaskAttemptContext context)
        {
            ensureSetup(context);
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
                return new RecordReaderBin(allowInputFailure);
            }
            System.out.println("AnyInput: using RecordReaderLine");
            return new RecordReaderLine(allowInputFailure);
        }
    }
    
    public static class BinInput extends ProtoInputFormat
    {
        public BinInput(int allowInputFailure)
        {
            super(allowInputFailure);
        }
        
        public BinInput(boolean allowEntireFileFail)
        {
            super(allowEntireFileFail);
        }
        
        public BinInput()
        {
            super(-1);
        }
        
        @Override
        public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split,
            org.apache.hadoop.mapreduce.TaskAttemptContext context)
        {
            ensureSetup(context);
            return new RecordReaderBin(allowInputFailure);
        }
    }
    
    public static class LineInput extends ProtoInputFormat
    {
        public LineInput(int allowInputFailure)
        {
            super(allowInputFailure);
        }
        
        public LineInput(boolean allowEntireFileFail)
        {
            super(allowEntireFileFail);
        }
        
        public LineInput()
        {
            super(-1);
        }
        
        @Override
        public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split,
            org.apache.hadoop.mapreduce.TaskAttemptContext context)
        {
            ensureSetup(context);
            return new RecordReaderLine(allowInputFailure);
        }
    }
    
    
    // Record readers:
    
    public static class RecordReaderBin
        extends org.apache.hadoop.mapreduce.RecordReader<LongWritable, BytesWritable>
    {
        int allowInputFailure = 0;
        long recnum = 0;
        LongWritable key = new LongWritable(0);
        BytesWritable value = new BytesWritable(new byte[1024]);
        CompressionCodecFactory compressionCodecs = null;
        DataInputStream in;
        long cur, end;
        
        public RecordReaderBin(int allowInputFailure)
        {
            this.allowInputFailure = allowInputFailure;
        }
        
        public RecordReaderBin()
        {
            this(0);
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
                if(allowInputFailure < Short.MAX_VALUE)
                {
                    throw e;
                }
                allowInputFailure = -2;
            }
        }
        
        @Override
        public boolean nextKeyValue() throws IOException
        {
            if(allowInputFailure == -2)
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
        int allowInputFailure = 0;
        boolean printedError = false;
        org.apache.hadoop.mapreduce.lib.input.LineRecordReader input;
        BytesWritable value;
        
        public RecordReaderLine(int allowInputFailure)
        {
            this.allowInputFailure = allowInputFailure;
            input = new org.apache.hadoop.mapreduce.lib.input.LineRecordReader();
        }
        
        public RecordReaderLine()
        {
            this(0);
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
                if(allowInputFailure < Short.MAX_VALUE)
                {
                    throw e;
                }
                allowInputFailure = -2;
            }
        }
        
        @Override
        public boolean nextKeyValue() throws IOException
        {
            if(allowInputFailure == -2)
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
                byte[] pbraw;
                try
                {
                    pbraw = Base64.decodeBase64(line.getBytes(), line.getLength());
                }
                catch(java.lang.ArrayIndexOutOfBoundsException oob)
                {
                    if(allowInputFailure > 0)
                    {
                        allowInputFailure--;
                        if(!printedError)
                        {
                            printedError = true;
                            System.out.println("Base64.decodeBase64 ArrayIndexOutOfBoundsException: " + oob.toString());
                        }
                        pbraw = new byte[0];
                    }
                    else
                    {
                        throw oob;
                    }
                }
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

