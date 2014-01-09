
package org.protobufloader;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.*;
import org.apache.pig.data.DataType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextOutputFormat;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.StorageUtil;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import org.apache.pig.tools.pigstats.PigStatusReporter;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ByteString;

import org.apache.hadoop.conf.Configuration;

import org.apache.commons.codec.binary.Base64;


@SuppressWarnings("unchecked")
public class ProtobufLoadFields extends FileInputLoadFunc implements LoadMetadata {
    protected RecordReader in = null;   
    //protected final Log mLog = LogFactory.getLog(getClass());
    protected String signature;
    PigSplit pigSplit;
        
    //private byte fieldDel = '\t';
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private BagFactory mBagFactory = BagFactory.getInstance();
    private String loadLocation;
    
    private String clsMapping;
    //List<LoadField> loadFields;
    Method newBuilderMethod;
    Schema schema = null;
    Configuration conf;
    protected LoadFieldTuple resultType;
    boolean resultNullable = false;
    
    public ProtobufLoadFields(String fields, String clsMapping) throws IOException {
        if(-1 != fields.indexOf("::"))
        {
            throw new IOException("Invalid schema: " + fields);
        }
        try
        {
            if(fields.equals("*"))
            {
            }
            else if(fields.equals("*?"))
            {
                resultNullable = true;
            }
            else
            {
                String xfields = fields.replace(".", "::").replace("?", "::IsNullable6274");
                //System.out.println("Fields: " + xfields);
                this.schema = Utils.getSchemaFromString(xfields); // Schema before repairing.
                if(this.schema == null)
                {
                    throw new IOException("Invalid schema: " + fields);
                }
                //System.out.println("Schema: " + this.schema.toString());
            }
        }
        /*catch(IOException e)
        {
            throw e;
        }*/
        catch(RuntimeException e)
        {
            throw e;
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
        this.clsMapping = clsMapping;
    }
    
    Object sameInputSplitObj = null;
    String cachedInputFilePath = "";
    String cachedInputSplitDateY = "", cachedInputSplitDateM = "",
        cachedInputSplitDateD = "", cachedInputSplitDateH = "";
    java.util.regex.Pattern inputSplitPattern = java.util.regex.Pattern
        .compile(".*\\b(\\d\\d\\d\\d)[-/](\\d\\d)[-/](\\d\\d)([-/](\\d\\d))?\\b.*"); // yyyy mm dd x hh
    
    Text lineText = null;
    int lineSize = -1; // Number of bytes in the line (e.g. base64 length).
    int msgSize = -1; // Number of bytes in the line's protobuf message.
    
    void updateInputInfo()
    {
        Object inputSplitObj = pigSplit.getWrappedSplit();
        if(inputSplitObj != sameInputSplitObj) // Object identity.
        {
            FileSplit inputSplit = (FileSplit)inputSplitObj;
            if(inputSplit != null)
            {
                String inputPath = inputSplit.getPath().toString();
                cachedInputFilePath = inputPath;
                System.out.println("Input: " + inputPath);
                java.util.regex.Matcher mx = inputSplitPattern.matcher(inputPath);
                if(mx.matches())
                {
                    String y = mx.group(1);
                    String m = mx.group(2);
                    String d = mx.group(3);
                    String h = mx.group(5);
                    if(h == null)
                    {
                        h = "";
                    }
                    // Cache it:
                    sameInputSplitObj = inputSplitObj;
                    cachedInputSplitDateY = y; cachedInputSplitDateM = m;
                    cachedInputSplitDateD = d; cachedInputSplitDateH = h;
                    return;
                }
            }
            cachedInputFilePath = "";
            cachedInputSplitDateY = ""; cachedInputSplitDateM = "";
            cachedInputSplitDateD = ""; cachedInputSplitDateH = "";
        }
    }
    
    @Override
    public Tuple getNext() throws IOException {
        boolean printedError = false;
        // Keep reading until successful return.
        try {
            while(in.nextKeyValue()) {
                try {
                    Text value = (Text) in.getCurrentValue();
                    byte[] wholebuf = value.getBytes();
                    int len = value.getLength();
                    lineSize = len;
                    lineText = value;
                    
                    updateInputInfo();
                    
                    //byte[] pbraw = Base64.decodeBase64(wholebuf, len);
                    byte[] goodbuf = wholebuf;
                    if(goodbuf.length != len)
                    {
                        goodbuf = new byte[len];
                        System.arraycopy(wholebuf, 0, goodbuf, 0, len);
                    }
                    byte[] pbraw = Base64.decodeBase64(goodbuf);
                    
                    msgSize = pbraw.length;
                    Message.Builder builder = newBuilder();
                    Message msg = builder.mergeFrom(pbraw).build();
                    
                    if(msg == null) {
                        PigStatusReporter rep = PigStatusReporter.getInstance();
                        rep.getCounter("ProtobufLoadFields", "ProtobufParseFail:_total").increment(1);
                        rep.getCounter("ProtobufLoadFields", "ProtobufParseFail:null").increment(1);
                    } else {
                        // Return result:
                        return getTuple(msg);
                    }
                } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                    PigStatusReporter rep = PigStatusReporter.getInstance();
                    rep.getCounter("ProtobufLoadFields", "ProtobufParseFail:_total").increment(1);
                    rep.getCounter("ProtobufLoadFields", "ProtobufParseFail:" + e.getMessage()).increment(1);
                    continue;
                } catch (InterruptedException e) {
                    int errCode = 6018;
                    String errMsg = "Error while reading input";
                    throw new ExecException(errMsg, errCode, 
                            PigException.REMOTE_ENVIRONMENT, e);
                } catch (Throwable e) {
                    if (!printedError) {
                        e.printStackTrace();
                        printedError = true;
                    }
                    PigStatusReporter rep = PigStatusReporter.getInstance();
                    rep.getCounter("ProtobufLoadFields", "throw:" + e.getClass().getName()).increment(1);
                    continue;
                }
            }
        }
        catch(IOException e)
        {
            throw e;
        }
        catch(RuntimeException e)
        {
            throw e;
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
        return null;
    }
    
    Message.Builder newBuilder() {
        try {
            return (Message.Builder)newBuilderMethod.invoke((Object)null, (Object[])null);
        }
        catch(RuntimeException e)
        {
            throw e;
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }
    
    protected Tuple getTuple(Message msg) throws IOException {
        return resultType.getValue(msg);
    }

    @Override
    public InputFormat getInputFormat() {
        if(loadLocation.endsWith(".bz2") || loadLocation.endsWith(".bz")) {
            return new Bzip2TextInputFormat();
        } else {
            return new PigTextInputFormat();
        }
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        this.in = reader;
        this.pigSplit = split;
                
        ensureResultType(split.getConf());
    }

    @Override
    public void setLocation(String location, Job job)
            throws IOException {
        loadLocation = location;
        FileInputFormat.setInputPaths(job, location);
    }
    
    @Override
    public void setUDFContextSignature(String signature) {
        this.signature = signature; 
    }
    
    // LoadMetadata
    @Override
    public String[] getPartitionKeys(String location, Job job) throws IOException {
        return null;
    }
    
    // LoadMetadata
    public ResourceSchema getSchema(String filename, Job job) throws IOException {
        ensureResultType(job.getConfiguration());
        return new ResourceSchema(resultType.pigSchema);
    }
    
    // LoadMetadata
    @Override
    public ResourceStatistics getStatistics(String location, Job job) throws IOException {
        return null;
    }
    
    // LoadMetadata
    @Override
    public void setPartitionFilter(Expression partitionFilter) throws IOException {
        /*
        getUDFContext().setProperty(
            PathPartitionHelper.PARITITION_FILTER_EXPRESSION,
            partitionFilter.toString());
        */
    }

	private Properties getUDFContext() {
		return UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[] { signature });
	}
    
    
    LoadFieldTuple repairMessageSchema(Message msg, Schema schema) throws IOException
    {
        return repairMessageSchema(null, msg.getDescriptorForType(), schema);
    }
    
    LoadFieldTuple repairMessageSchema(String[] parent, Descriptors.Descriptor desc, Schema schema) throws IOException
    {
        LoadFieldTuple result = new LoadFieldTuple(this.mTupleFactory);
        
        if(schema == null)
        {
            schema = new Schema();
            //Descriptors.Descriptor xdesc = msg.getDescriptorForType();
            Descriptors.Descriptor xdesc = desc;
            List<Descriptors.FieldDescriptor> xfields = xdesc.getFields();
            for(Descriptors.FieldDescriptor xfd : xfields)
            {
                schema.add(new Schema.FieldSchema(xfd.getName(), DataType.UNKNOWN));
            }
        }
        
        Schema newSchema = new Schema();
        int nfields = schema.size();
        for(int i = 0; i < nfields; i++)
        {
            Schema.FieldSchema f = schema.getField(i);
            String name = f.alias;
            boolean nullable = false;
            if(name.endsWith("::IsNullable6274"))
            {
                nullable = true;
                name = name.substring(0, name.length() - "::IsNullable6274".length());
            }
            String[] subProtoFieldPath = name.split("::");
            String[] protoFieldPath = subProtoFieldPath;
            /*
            if(parent != null && parent.length > 0)
            {
                protoFieldPath = new String[parent.length + subProtoFieldPath.length];
                for(int k = 0; k < parent.length; k++)
                {
                    protoFieldPath[k] = parent[k];
                }
                for(int k = 0; k < subProtoFieldPath.length; k++)
                {
                    protoFieldPath[parent.length + k] = subProtoFieldPath[k];
                }
            }
            */
            //Message curmsg = msg;
            Descriptors.Descriptor curdesc = desc;
            for(int j = 0; j < subProtoFieldPath.length; j++)
            {
                //Descriptors.Descriptor curdesc = curmsg.getDescriptorForType();
                final Descriptors.FieldDescriptor fd = curdesc.findFieldByName(subProtoFieldPath[j]);
                if(fd == null)
                {
                    LoadField special = null;
                    if(parent == null && protoFieldPath.length == 1)
                    {
                        if("year".equals(name))
                        {
                            special = new SpecialField_year();
                        }
                        else if("month".equals(name))
                        {
                            special = new SpecialField_month();
                        }
                        else if("day".equals(name))
                        {
                            special = new SpecialField_day();
                        }
                        else if("hour".equals(name))
                        {
                            special = new SpecialField_hour();
                        }
                        else if("inputFile".equals(name))
                        {
                            special = new SpecialField_inputFile();
                        }
                        else if("rowInc".equals(name))
                        {
                            special = new SpecialField_rowInc();
                        }
                        else if("msgString".equals(name))
                        {
                            special = new SpecialField_msgString();
                        }
                        else if("lineSize".equals(name))
                        {
                            special = new SpecialField_lineSize();
                        }
                        else if("msgSize".equals(name))
                        {
                            special = new SpecialField_msgSize();
                        }
                        else if("rawLine".equals(name))
                        {
                            special = new SpecialField_rawLine();
                        }
                    }
                    if(special != null)
                    {
                        name = "special::" + name;
                        protoFieldPath = name.split("::");
                        special.protoFieldPath = protoFieldPath;
                        special.pigFieldSchema = new Schema.FieldSchema(name, special.pigType);
                        newSchema.add(special.pigFieldSchema);
                        result.fields.add(special);
                        continue;
                    }
                    else
                    {
                        throw new IOException("Cannot find field " + subProtoFieldPath[j] + " from " + java.util.Arrays.toString(protoFieldPath));
                    }
                }
                //final Object v = curmsg.getField(fd);
                //final Object v = fd.getDefaultValue(); // FieldDescriptor.getDefaultValue() called on an embedded message field.
                if(j < subProtoFieldPath.length - 1)
                {
                    /*
                    final Object v = fd.getMessageType();
                    if(!(v instanceof Message))
                    {
                        throw new RuntimeException("Cannot descend into " + subProtoFieldPath[j] + ": " + name
                            + "  -  got: " + v.toString());
                    }
                    //curmsg = (Message)v;
                    curdesc = ((Message)v).getDescriptorForType();
                    */
                    final Object v = fd.getMessageType();
                    if(!(v instanceof Descriptors.Descriptor))
                    {
                        throw new RuntimeException("Cannot descend into " + subProtoFieldPath[j] + ": " + name
                            + "  -  got: " + v.toString());
                    }
                    curdesc = ((Descriptors.Descriptor)v);
                }
                else
                {
                    LoadField lf = null;
                    //if(v instanceof Message or v instanceof List)
                    //Descriptors.FieldDescriptor.Type ft = fd.getType();
                    Descriptors.FieldDescriptor.JavaType ft = fd.getJavaType();
                    String fieldClassName = null; // Null if no class.
                    if(ft == Descriptors.FieldDescriptor.JavaType.MESSAGE)
                    {
                        //Message vmsg = (Message)v;
                        Descriptors.Descriptor vdesc = fd.getMessageType();
                        fieldClassName = vdesc.getName();
                        Schema xschema = null;
                        if(f.schema != null)
                        {
                            if(f.schema.size() == 1 && f.schema.getField(0).type == DataType.TUPLE)
                            {
                                xschema = f.schema.getField(0).schema;
                            }
                            else
                            {
                                xschema = f.schema;
                            }
                        }
                        //LoadFieldTuple lft = repairMessageSchema(protoFieldPath, vmsg, xschema);
                        LoadFieldTuple lft = repairMessageSchema(protoFieldPath, vdesc, xschema);
                        lf = lft;
                        lf.pigFieldSchema = new Schema.FieldSchema(name, lft.pigSchema);
                    }
                    else
                    {
                        //final Object v = fd.getDefaultValue();
                        //if(v instanceof String)
                        if(ft == Descriptors.FieldDescriptor.JavaType.STRING)
                        {
                            lf = new LoadFieldScalar<String>();
                            lf.pigType = DataType.CHARARRAY;
                        }
                        //else if(v instanceof Integer)
                        else if(ft == Descriptors.FieldDescriptor.JavaType.INT)
                        {
                            lf = new LoadFieldScalar<Integer>();
                            lf.pigType = DataType.INTEGER;
                        }
                        //else if(v instanceof Long)
                        else if(ft == Descriptors.FieldDescriptor.JavaType.LONG)
                        {
                            lf = new LoadFieldScalar<Long>();
                            lf.pigType = DataType.LONG;
                        }
                        //else if(v instanceof Float)
                        else if(ft == Descriptors.FieldDescriptor.JavaType.FLOAT)
                        {
                            lf = new LoadFieldScalar<Float>();
                            lf.pigType = DataType.FLOAT;
                        }
                        //else if(v instanceof Double)
                        else if(ft == Descriptors.FieldDescriptor.JavaType.DOUBLE)
                        {
                            lf = new LoadFieldScalar<Double>();
                            lf.pigType = DataType.DOUBLE;
                        }
                        //else if(v instanceof Boolean)
                        else if(ft == Descriptors.FieldDescriptor.JavaType.BOOLEAN)
                        {
                            if(f.type == DataType.BOOLEAN)
                            {
                                lf = new LoadFieldScalar<Boolean>();
                                lf.pigType = DataType.BOOLEAN;
                            }
                            else
                            {
                                lf = new LoadFieldBooleanToInt();
                            }
                        }
                        //else if(v instanceof Descriptors.EnumValueDescriptor)
                        else if(ft == Descriptors.FieldDescriptor.JavaType.ENUM)
                        {
                            if(f.type == DataType.INTEGER)
                            {
                                lf = new LoadFieldEnumToInt();
                                lf.pigType = DataType.INTEGER;
                            }
                            else
                            {
                                lf = new LoadFieldEnum();
                            }
                        }
                        else if(ft == Descriptors.FieldDescriptor.JavaType.BYTE_STRING)
                        {
                            if(f.type == DataType.CHARARRAY)
                            {
                                lf = new LoadFieldBytesToString();
                                lf.pigType = DataType.CHARARRAY;
                            }
                            else
                            {
                                lf = new LoadFieldBytes();
                            }
                        }
                        if(lf != null)
                        {
                            lf.pigFieldSchema = new Schema.FieldSchema(name, lf.pigType);
                        }
                        // If repeated, wrap in a tuple and then in a bag...
                    }
                    if(lf == null)
                    {
                        //throw new IOException("Unhandled type found in protobuf: " + name + " of type: " + v.getClass().getName());
                        throw new IOException("Unhandled type found in protobuf: " + name);
                    }
                    lf.nullable = nullable;
                    lf.protoFieldPath = protoFieldPath;
                    if(fd.isRepeated())
                    {
                        LoadFieldBag lfbag = new LoadFieldBag(mBagFactory);
                        LoadFieldTuple lftuple;
                        if(lf instanceof LoadFieldTuple)
                        {
                            lftuple = (LoadFieldTuple)lf;
                        }
                        else
                        {
                            lftuple = new LoadFieldTuple(mTupleFactory, true);
                            lftuple.fields.add(lf);
                            lftuple.pigSchema = new Schema(lf.pigFieldSchema);
                            lftuple.pigFieldSchema = new Schema.FieldSchema(name, lftuple.pigSchema);
                        }
                        lfbag.fields.add(lftuple);
                        lfbag.pigSchema = new Schema(new Schema.FieldSchema("t", lftuple.pigSchema));
                        lfbag.pigSchema.setTwoLevelAccessRequired(true);
                        lfbag.pigFieldSchema = new Schema.FieldSchema(name, lfbag.pigSchema, DataType.BAG);
                        lf = lfbag;
                        boolean bagToMap = f.type == DataType.MAP; // Default to bag..
                        if(fieldClassName != null && -1 != fieldClassName.indexOf("CountedMap"))
                        {
                            // But if CountedMap, default to map unless explicitly bag.
                            bagToMap = f.type != DataType.BAG;
                        }
                        if(bagToMap)
                        {
                            LoadFieldBagToMap lfmap = new LoadFieldBagToMap(lfbag);
                            lfmap.pigType = DataType.MAP;
                            lfmap.pigFieldSchema = new Schema.FieldSchema(name, lfmap.pigType);
                            lf = lfmap;
                        }
                    }
                    newSchema.add(lf.pigFieldSchema);
                    result.fields.add(lf);
                    break;
                }
            }
        }
        result.pigSchema = newSchema;
        return result;
    }


    static class LoadField<T>
    {
        byte pigType = DataType.ERROR;
        Schema.FieldSchema pigFieldSchema;
        boolean nullable = false;
        
        String[] protoFieldPath; // Protobuf path.
        Descriptors.FieldDescriptor protoField; // isOptional, isRepeated, etc.
        //Descriptors.FieldDescriptor.Type protoFieldType; // Descriptors.FieldDescriptor.getType()
        
        LoadField()
        {
            //this.pigType = DataType.findType(T.class);
        }
        
        @Override
        public String toString()
        {
            if(pigFieldSchema != null)
            {
                return super.toString() + "@" + pigFieldSchema.toString();
            }
            return super.toString();
        }
        
        T getValue(Message msg) throws IOException
        {
            return null;
        }
        
        T getValueRepeated(Message msg, int index) throws IOException
        {
            String name = "N/A";
            if(pigFieldSchema != null && pigFieldSchema.alias != null)
            {
                name = pigFieldSchema.alias;
            }
            throw new RuntimeException("Repeated not implemented for " + name);
            //return null;
        }
        
        // Descends into messages specified by protoFieldPath.
        static Message descend(Message msg, String[] protoFieldPath, int until) throws IOException
        {
            if(protoFieldPath.length == 0)
            {
                throw new RuntimeException("Nothing to descend");
            }
            for(int i = 0; i < protoFieldPath.length + until; i++)
            {
                Descriptors.Descriptor desc = msg.getDescriptorForType();
                Descriptors.FieldDescriptor fd = desc.findFieldByName(protoFieldPath[i]);
                if(fd == null)
                {
                    throw new IOException("Cannot descend into " + protoFieldPath[i] + " from " + java.util.Arrays.toString(protoFieldPath));
                }
                //Descriptors.FieldDescriptor.JavaType ft = fd.getJavaType();
                Object value = msg.getField(fd);
                if(!(value instanceof Message))
                {
                    //if(i != protoFieldPath.length - 1)
                    {
                        throw new RuntimeException("Cannot descend into "
                            + protoFieldPath[i] + " from " + java.util.Arrays.toString(protoFieldPath)
                            + " - type is : " + value.getClass().getName()
                            );
                    }
                    //return msg;
                }
                msg = (Message)value;
            }
            return msg;
        }
        
        Message descend(Message msg, int until) throws IOException
        {
            return descend(msg, this.protoFieldPath, until);
        }
        
    }
    
    // Bag or tuple.
    static class LoadFieldNestable<T> extends LoadField<T>
    {
        Schema pigSchema;
        
        List<LoadField> fields = new ArrayList<LoadField>();
        
        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(super.toString());
            sb.append('(');
            for(int i = 0; i < fields.size(); i++)
            {
                if(i != 0)
                {
                    sb.append(',');
                }
                sb.append(fields.get(i).toString());
            }
            sb.append(')');
            return sb.toString();
        }
        
    }
    
    static class LoadFieldBag extends LoadFieldNestable<DataBag>
    {
        BagFactory mBagFactory;
        
        LoadFieldBag(BagFactory bagFactory)
        {
            super();
            pigType = DataType.BAG;
            this.mBagFactory = bagFactory;
        }
        
        @Override
        DataBag getValue(Message msg) throws IOException
        {
            if(fields.size() != 1 || !(fields.get(0) instanceof LoadFieldTuple))
            {
                throw new RuntimeException("LoadFieldBag expects one field of type LoadFieldTuple");
            }
            //LoadField tuple = fields.get(0);
            LoadFieldTuple tuple = (LoadFieldTuple)fields.get(0);
            DataBag result = mBagFactory.newDefaultBag();
            for(int i = 0;; i++)
            {
                Tuple row = tuple.getValueRepeated(msg, i);
                if(row == null)
                {
                    break;
                }
                result.add(row);
            }
            return result;
        }
        
    }
    
    static class LoadFieldBagToMap extends LoadField<Map>
    {
        LoadFieldTuple bagTuple;
        
        LoadFieldBagToMap(List<LoadField> bagFields)
        {
            super();
            if(bagFields.size() != 1 || !(bagFields.get(0) instanceof LoadFieldTuple))
            {
                throw new RuntimeException("LoadFieldBagToMap bag expects one field of type LoadFieldTuple");
            }
            //LoadField tuple = bagFields.get(0);
            LoadFieldTuple tuple = (LoadFieldTuple)bagFields.get(0);
            if(tuple.fields.size() == 0 || tuple.fields.size() > 2)
            {
                throw new RuntimeException("Map needs 1 or 2 fields for key=value");
            }
            bagTuple = tuple;
        }
        
        LoadFieldBagToMap(LoadFieldBag bag)
        {
            this(bag.fields);
        }
        
        @Override
        Map getValue(Message msg) throws IOException
        {
            HashMap result = new HashMap();
            LoadFieldTuple tuple = bagTuple;
            for(int i = 0;; i++)
            {
                Tuple row = tuple.getValueRepeated(msg, i);
                if(row == null)
                {
                    break;
                }
                Object k = row.get(0);
                Object v;
                if(row.size() > 1)
                {
                    v = row.get(1);
                }
                else
                {
                    v = (Long)1L;
                }
                Object oldv = result.get(k);
                // Always test the types so it fails early.
                if(v instanceof String)
                {
                    if(oldv != null)
                    {
                        v = ((String)oldv + "," + (String)v);
                    }
                }
                else if(v instanceof Long)
                {
                    if(oldv != null)
                    {
                        v = ((long)(Long)oldv + (long)(Long)v);
                    }
                }
                else if(v instanceof Double)
                {
                    if(oldv != null)
                    {
                        v = ((double)(Double)oldv + (double)(Double)v);
                    }
                }
                else
                {
                    throw new IOException("Cannot convert bag to Map<"
                        + k.getClass().getName()
                        + ", "
                        + v.getClass().getName()
                        + ">"
                        );
                }
                result.put(k, v);
            }
            return result;
        }
    }
    
    // Note: a tuple itself won't be nullable, but if it is marked as nullable, all its fields should be nullable.
    static class LoadFieldTuple extends LoadFieldNestable<Tuple>
    {
        TupleFactory mTupleFactory;
        boolean created = false; // A created tuple is one that doesn't have a physical protobuf message.
        
        LoadFieldTuple(TupleFactory tupleFactory)
        {
            super();
            pigType = DataType.TUPLE;
            this.mTupleFactory = tupleFactory;
        }
        
        LoadFieldTuple(TupleFactory tupleFactory, boolean created)
        {
            this(tupleFactory);
            this.created = created;
        }
        
        Tuple _getValue(Message msg, int how) throws IOException
        {
            int nfields = pigSchema.size();
            if(nfields != fields.size())
            {
                throw new RuntimeException("Field count mismatch (pigSchema != fields)");
            }
            Tuple t = mTupleFactory.newTuple(nfields);
            if(created || protoFieldPath == null)
            {
                for(int i = 0; i < nfields; i++)
                {
                    LoadField lf = fields.get(i);
                    lf.nullable = lf.nullable || this.nullable; // Propagate nullable.
                    if(how >= 0)
                    {
                        Object x = lf.getValueRepeated(msg, how);
                        if(x == null)
                        {
                            return null;
                        }
                        t.set(i, x);
                    }
                    else
                    {
                        t.set(i, lf.getValue(msg));
                    }
                }
            }
            else
            {
                if(protoFieldPath != null)
                {
                    if(how >= 0)
                    {
                        msg = descend(msg, -1);
                        Descriptors.Descriptor desc = msg.getDescriptorForType();
                        Descriptors.FieldDescriptor fd = desc.findFieldByName(protoFieldPath[protoFieldPath.length - 1]);
                        if(fd == null)
                        {
                            throw new IOException("Tuple not found: " + java.util.Arrays.toString(protoFieldPath));
                        }
                        Descriptors.FieldDescriptor.JavaType ft = fd.getJavaType();
                        if(ft != Descriptors.FieldDescriptor.JavaType.MESSAGE)
                        {
                            throw new IOException("Not a tuple: " + java.util.Arrays.toString(protoFieldPath));
                        }
                        if(how >= msg.getRepeatedFieldCount(fd))
                        {
                            return null;
                        }
                        msg = (Message)msg.getRepeatedField(fd, how);
                    }
                    else
                    {
                        msg = descend(msg, 0);
                    }
                }
                for(int i = 0; i < nfields; i++)
                {
                    LoadField lf = fields.get(i);
                    lf.nullable = lf.nullable || this.nullable; // Propagate nullable.
                    t.set(i, lf.getValue(msg));
                }
            }
            return t;
        }
        
        @Override
        Tuple getValue(Message msg) throws IOException
        {
            return _getValue(msg, -1);
        }
        
        @Override
        Tuple getValueRepeated(Message msg, int index) throws IOException
        {
            return _getValue(msg, index);
        }
    }
    
    static class LoadFieldScalar<T> extends LoadField<T>
    {
        // how -1 = get value or default value.
        // how -2 = get value or null.
        // how >= 0 = get repeated index, or null if at end.
        static <T> T getValue(Message msg, String[] protoFieldPath, int how) throws IOException
        {
            Message parentmsg = descend(msg, protoFieldPath, -1);
            Descriptors.Descriptor parentdesc = parentmsg.getDescriptorForType();
            Descriptors.FieldDescriptor fd = parentdesc.findFieldByName(protoFieldPath[protoFieldPath.length - 1]);
            if(fd == null)
            {
                throw new IOException("Field not found: " + java.util.Arrays.toString(protoFieldPath));
            }
            if(how >= 0)
            {
                if(how >= parentmsg.getRepeatedFieldCount(fd))
                {
                    return null;
                }
                return (T)parentmsg.getRepeatedField(fd, how);
            }
            if(how == -2)
            {
                if(!parentmsg.hasField(fd))
                {
                    return null;
                }
            }
            return (T)parentmsg.getField(fd);
        }
        
        @Override
        T getValue(Message msg) throws IOException
        {
            return LoadFieldScalar.<T>getValue(msg, this.protoFieldPath, this.nullable ? -2 : -1);
        }
        
        @Override
        T getValueRepeated(Message msg, int index) throws IOException
        {
            return LoadFieldScalar.<T>getValue(msg, this.protoFieldPath, index);
        }
    }
    
    static class LoadFieldBooleanToInt extends LoadFieldScalar<Integer>
    {
        LoadFieldBooleanToInt()
        {
            super();
            this.pigType = DataType.INTEGER;
        }
        
        static Integer getValue(Message msg, String[] protoFieldPath, int how) throws IOException
        {
            Boolean ob = LoadFieldScalar.<Boolean>getValue(msg, protoFieldPath, how);
            if(ob == null)
            {
                return null;
            }
            boolean b = ob;
            return b ? 1 : 0;
        }
        
        @Override
        Integer getValue(Message msg) throws IOException
        {
            return getValue(msg, this.protoFieldPath, this.nullable ? -2 : -1);
        }
        
        @Override
        Integer getValueRepeated(Message msg, int index) throws IOException
        {
            return getValue(msg, this.protoFieldPath, index);
        }
    }
    
    static class LoadFieldBytes extends LoadFieldScalar<DataByteArray>
    {
        LoadFieldBytes()
        {
            super();
            this.pigType = DataType.BYTEARRAY;
        }
        
        static ByteString getValue(Message msg, String[] protoFieldPath, int how) throws IOException
        {
            ByteString bs = LoadFieldScalar.<ByteString>getValue(msg, protoFieldPath, how);
            if(bs == null)
            {
                return null;
            }
            return bs;
        }
        
        @Override
        DataByteArray getValue(Message msg) throws IOException
        {
            ByteString bs = getValue(msg, this.protoFieldPath, this.nullable ? -2 : -1);
            if(bs == null)
            {
                return null;
            }
            return new DataByteArray(bs.toByteArray());
        }
        
        @Override
        DataByteArray getValueRepeated(Message msg, int index) throws IOException
        {
            ByteString bs = getValue(msg, this.protoFieldPath, index);
            if(bs == null)
            {
                return null;
            }
            return new DataByteArray(bs.toByteArray());
        }
    }
    
    static class LoadFieldBytesToString extends LoadFieldScalar<String>
    {
        LoadFieldBytesToString()
        {
            super();
            this.pigType = DataType.CHARARRAY;
        }
        
        static String getValue(Message msg, String[] protoFieldPath, int how) throws IOException
        {
            ByteString bs = LoadFieldBytes.getValue(msg, protoFieldPath, how);
            if(bs == null)
            {
                return null;
            }
            return bs.toStringUtf8();
        }
        
        @Override
        String getValue(Message msg) throws IOException
        {
            return getValue(msg, this.protoFieldPath, this.nullable ? -2 : -1);
        }
        
        @Override
        String getValueRepeated(Message msg, int index) throws IOException
        {
            return getValue(msg, this.protoFieldPath, index);
        }
    }
    
    static class LoadFieldEnum extends LoadFieldScalar<String>
    {
        LoadFieldEnum()
        {
            super();
            this.pigType = DataType.CHARARRAY;
        }
        
        static Descriptors.EnumValueDescriptor getValueEnum(Message msg, String[] protoFieldPath, int how) throws IOException
        {
            return LoadFieldScalar.<Descriptors.EnumValueDescriptor>getValue(msg, protoFieldPath, how);
        }
        
        static String getValue(Message msg, String[] protoFieldPath, int how) throws IOException
        {
            Descriptors.EnumValueDescriptor vv = getValueEnum(msg, protoFieldPath, how);
            if(vv == null)
            {
                return null;
            }
            return vv.getName();
        }
        
        @Override
        String getValue(Message msg) throws IOException
        {
            return getValue(msg, this.protoFieldPath, this.nullable ? -2 : -1);
        }
        
        @Override
        String getValueRepeated(Message msg, int index) throws IOException
        {
            return getValue(msg, this.protoFieldPath, index);
        }
    }
    
    static class LoadFieldEnumToInt extends LoadFieldScalar<Integer>
    {
        LoadFieldEnumToInt()
        {
            super();
            this.pigType = DataType.INTEGER;
        }
        
        static Integer getValue(Message msg, String[] protoFieldPath, int how) throws IOException
        {
            Descriptors.EnumValueDescriptor vv = LoadFieldEnum.getValueEnum(msg, protoFieldPath, how);
            if(vv == null)
            {
                return null;
            }
            return vv.getNumber();
        }
        
        @Override
        Integer getValue(Message msg) throws IOException
        {
            return getValue(msg, this.protoFieldPath, this.nullable ? -2 : -1);
        }
        
        @Override
        Integer getValueRepeated(Message msg, int index) throws IOException
        {
            return getValue(msg, this.protoFieldPath, index);
        }
    }
    
    
    // Note: not a static class.
    class SpecialField_year extends LoadField<String>
    {
        SpecialField_year()
        {
            pigType = DataType.CHARARRAY;
        }
        @Override
        String getValue(Message msg) throws IOException
        {
            //updateInputInfo();
            return cachedInputSplitDateY;
        }
    }
    class SpecialField_month extends LoadField<String>
    {
        SpecialField_month()
        {
            pigType = DataType.CHARARRAY;
        }
        @Override
        String getValue(Message msg) throws IOException
        {
            //updateInputInfo();
            return cachedInputSplitDateM;
        }
    }
    class SpecialField_day extends LoadField<String>
    {
        SpecialField_day()
        {
            pigType = DataType.CHARARRAY;
        }
        @Override
        String getValue(Message msg) throws IOException
        {
            //updateInputInfo();
            return cachedInputSplitDateD;
        }
    }
    class SpecialField_hour extends LoadField<String>
    {
        SpecialField_hour()
        {
            pigType = DataType.CHARARRAY;
        }
        @Override
        String getValue(Message msg) throws IOException
        {
            //updateInputInfo();
            return cachedInputSplitDateH;
        }
    }
    class SpecialField_inputFile extends LoadField<String>
    {
        SpecialField_inputFile()
        {
            pigType = DataType.CHARARRAY;
        }
        @Override
        String getValue(Message msg) throws IOException
        {
            //updateInputInfo();
            return cachedInputFilePath;
        }
    }
    class SpecialField_rowInc extends LoadField<Integer>
    {
        int taskID = -1;
        int numMapTasks = 0;
        int _nextID = 0;
        int _maxID = 0;
        SpecialField_rowInc()
        {
            pigType = DataType.INTEGER;
        }
        @Override
        Integer getValue(Message msg) throws IOException
        {
            if(taskID == -1)
            {
                numMapTasks = conf.getInt("mapred.map.tasks", 0);
                if(numMapTasks < 1)
                {
                    throw new RuntimeException("rowInc: unable to get mapred.map.tasks");
                }
                taskID = org.apache.hadoop.mapreduce.TaskAttemptID.forName(conf.get("mapred.task.id")).getTaskID().getId();
                if(taskID >= numMapTasks)
                {
                    throw new RuntimeException("rowInc: taskID out of bounds (taskID=" + taskID + " numMapTasks=" + numMapTasks + ")");
                }
                int rx = 0x7FFFFFFF / numMapTasks;
                _nextID = rx * taskID;
                _maxID = _nextID + rx - 1;
                //_warnID = _nextID + (rx / 4 * 3) - 1;
            }
            int id = _nextID;
            if(id >= _maxID)
            {
                throw new RuntimeException("rowInc: Too many rows in map taskID " + taskID);
            }
            _nextID++;
            return id;
        }
    }
    class SpecialField_msgString extends LoadField<String>
    {
        SpecialField_msgString()
        {
            pigType = DataType.CHARARRAY;
        }
        @Override
        String getValue(Message msg) throws IOException
        {
            return msg.toString();
        }
    }
    class SpecialField_lineSize extends LoadField<Integer>
    {
        SpecialField_lineSize()
        {
            pigType = DataType.INTEGER;
        }
        @Override
        Integer getValue(Message msg) throws IOException
        {
            return lineSize;
        }
    }
    class SpecialField_msgSize extends LoadField<Integer>
    {
        SpecialField_msgSize()
        {
            pigType = DataType.INTEGER;
        }
        @Override
        Integer getValue(Message msg) throws IOException
        {
            return msgSize;
        }
    }
    class SpecialField_rawLine extends LoadField<String>
    {
        SpecialField_rawLine()
        {
            pigType = DataType.CHARARRAY;
        }
        @Override
        String getValue(Message msg) throws IOException
        {
            return lineText.toString();
        }
    }


    void ensureResultType(Configuration conf) throws IOException
    {
        this.conf = conf;
        
        if(newBuilderMethod == null)
        {
            try {
                Class<? extends Message> cls = (Class<? extends Message>)loadProtoClass(clsMapping, conf);
                newBuilderMethod = cls.getMethod("newBuilder", new Class[] {});
            } catch (RuntimeException e) {
                throw e;
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
        
        if(resultType == null)
        {
            Message.Builder builder = newBuilder();
            Message msg = builder.getDefaultInstanceForType();
            resultType = repairMessageSchema(msg, this.schema);
            resultType.nullable = resultType.nullable || resultNullable;
        }
    }
    

    @SuppressWarnings("unchecked")
    static Class<? extends Message> loadProtoClass(String clsMapping,
                    Configuration conf) {

            String protoClassName = null;
            
            if (conf != null) {
                protoClassName = conf.get(clsMapping);
            }

            if (protoClassName == null) {
                    //throw new RuntimeException("No property defined for " + clsMapping);
                    protoClassName = clsMapping;
            }

            Class<? extends Message> protoClass = null;

            try {
                    protoClass = (Class<? extends Message>) Thread.currentThread()
                                    .getContextClassLoader().loadClass(protoClassName);
            } catch (ClassNotFoundException e) {
                    throw new RuntimeException("Error instantiating " + protoClassName
                                    + " referred to by " + clsMapping, e);
            }

            //LOG.info("Using " + protoClass.getName() + " mapped by " + clsMapping);

            return protoClass;

    }


}

