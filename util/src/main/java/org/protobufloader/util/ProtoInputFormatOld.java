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
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.Configuration;


public abstract class ProtoInputFormatOld
        extends org.apache.hadoop.mapred.FileInputFormat<LongWritable, BytesWritable>
        implements JobConfigurable
{
    protected int allowInputFailure = -1;
    private CompressionCodecFactory compressionCodecs = null;
    
    public ProtoInputFormatOld(int allowInputFailure) throws IOException
    {
        this.allowInputFailure = allowInputFailure;
    }
    
    public ProtoInputFormatOld(boolean allowEntireFileFail) throws IOException
    {
        this(allowEntireFileFail ? Short.MAX_VALUE : 0);
    }
    
    public ProtoInputFormatOld() throws IOException
    {
        this(-1);
    }
    
    protected void ensureSetup(JobConf job)
    {
        if(allowInputFailure == -1)
        {
            allowInputFailure = job.getInt("protobufloader.input.failures.allow", 0);
        }
    }
    
    public void configure(JobConf job)
    {
        compressionCodecs = new CompressionCodecFactory(job);
    }
    
    @Override
    protected boolean isSplitable(org.apache.hadoop.fs.FileSystem fs, Path file)
    {
        return compressionCodecs.getCodec(file) == null;
    }
    
    @Override
    protected org.apache.hadoop.fs.FileStatus[] listStatus(JobConf job) throws IOException
    {
        return getAllFileRecursively(super.listStatus(job), job);
    }
    
    
    // Input formats:
    
    // Any supported based on runtime conditions.
    public static class AnyInput extends ProtoInputFormatOld
    {
        public AnyInput(int allowInputFailure) throws IOException
        {
            super(allowInputFailure);
        }
        
        public AnyInput(boolean allowEntireFileFail) throws IOException
        {
            super(allowEntireFileFail);
        }
        
        public AnyInput() throws IOException
        {
            super(-1);
        }
        
        @Override
        public RecordReader<LongWritable, BytesWritable> getRecordReader(InputSplit split,
            JobConf job, Reporter reporter) throws IOException
        {
            ensureSetup(job);
            boolean isbin = false;
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
                return new RecordReaderBin(allowInputFailure, job, split);
            }
            System.out.println("AnyInput: using RecordReaderLine");
            return new RecordReaderLine(allowInputFailure, job, split);
        }
    }
    
    public static class BinInput extends ProtoInputFormatOld
    {
        public BinInput(int allowInputFailure) throws IOException
        {
            super(allowInputFailure);
        }
        
        public BinInput(boolean allowEntireFileFail) throws IOException
        {
            super(allowEntireFileFail);
        }
        
        public BinInput() throws IOException
        {
            super(-1);
        }
        
        @Override
        public RecordReader<LongWritable, BytesWritable> getRecordReader(InputSplit split,
            JobConf job, Reporter reporter) throws IOException
        {
            ensureSetup(job);
            return new RecordReaderBin(allowInputFailure, job, split);
        }
    }
    
    public static class LineInput extends ProtoInputFormatOld
    {
        public LineInput(int allowInputFailure) throws IOException
        {
            super(allowInputFailure);
        }
        
        public LineInput(boolean allowEntireFileFail) throws IOException
        {
            super(allowEntireFileFail);
        }
        
        public LineInput() throws IOException
        {
            super(-1);
        }
        
        @Override
        public RecordReader<LongWritable, BytesWritable> getRecordReader(InputSplit split,
            JobConf job, Reporter reporter) throws IOException
        {
            ensureSetup(job);
            return new RecordReaderLine(allowInputFailure, job, split);
        }
    }
    
    
    // Record readers:
    
    public static class RecordReaderBin
        implements org.apache.hadoop.mapred.RecordReader<LongWritable, BytesWritable>
    {
        int allowInputFailure = 0;
        long recnum = 0;
        CompressionCodecFactory compressionCodecs = null;
        DataInputStream in;
        long cur, end;
        
        public RecordReaderBin(int allowInputFailure, JobConf job, InputSplit split) throws IOException
        {
            this.allowInputFailure = allowInputFailure;
            initialize(split, job);
        }
        
        public RecordReaderBin(JobConf job, InputSplit split) throws IOException
        {
            this(0, job, split);
        }
        
        public LongWritable createKey()
        {
            return new LongWritable(0);
        }
        
        public BytesWritable createValue()
        {
            return new BytesWritable();
        }
        
        private void initialize(InputSplit genericSplit, JobConf job) throws IOException
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
        
        public boolean next(LongWritable key, BytesWritable value) throws IOException
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
        
        public long getPos() throws IOException
        {
            if(cur == 0)
            {
                return 0;
            }
            return cur; // From entire split.
        }
        
        public float getProgress() throws IOException
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
        implements org.apache.hadoop.mapred.RecordReader<LongWritable, BytesWritable>
    {
        int allowInputFailure = 0;
        boolean printedError = false;
        org.apache.hadoop.mapred.LineRecordReader input;
        Text tempvalue;
        
        public RecordReaderLine(int allowInputFailure, JobConf job, InputSplit split) throws IOException
        {
            this.allowInputFailure = allowInputFailure;
            initialize(split, job);
        }
        
        public RecordReaderLine(JobConf job, InputSplit split) throws IOException
        {
            this(0, job, split);
        }
        
        public LongWritable createKey()
        {
            return new LongWritable(0);
        }
        
        public BytesWritable createValue()
        {
            return new BytesWritable();
        }
        
        private void initialize(InputSplit genericSplit, JobConf job) throws IOException
        {
            try
            {
                FileSplit split = (FileSplit)genericSplit;
                if(split.getStart() != 0)
                {
                    System.out.println("ProtoRecordReaderLine is reading from split input");
                }
                input = new org.apache.hadoop.mapred.LineRecordReader(job, split);
                tempvalue = input.createValue();
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
        
        public boolean next(LongWritable key, BytesWritable value) throws IOException
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
            if(input.next(key, tempvalue))
            {
                Text line = tempvalue;
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
                value.set(pbraw, 0, pbraw.length);
                return true;
            }
            return false;
        }
        
        public long getPos() throws IOException
        {
            return input.getPos();
        }
        
        public float getProgress() throws IOException
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
    public static FileStatus[] getAllFileRecursively(
            FileStatus[] files, JobConf conf) throws IOException {
        List<FileStatus> result = new ArrayList<FileStatus>();
        int len = files.length;
        for (int i = 0; i < len; ++i) {
            FileStatus file = files[i];
            if (file.isDir()) {
                Path p = file.getPath();
                org.apache.hadoop.fs.FileSystem fs = p.getFileSystem(conf);
                addInputPathRecursively(result, fs, p, hiddenFileFilter);
            } else {
                result.add(file);
            }
        }
        System.out.println("Total input paths to process : " + result.size()); 
        //return result;
        return result.toArray(new FileStatus[result.size()]);
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

