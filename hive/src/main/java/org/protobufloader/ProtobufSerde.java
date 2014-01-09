package org.protobufloader;

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.lang.reflect.Method;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde.serdeConstants;
//import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
//import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.AbstractDeserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;

import com.google.protobuf.Message;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ByteString;

import org.apache.commons.codec.binary.Base64;


public class ProtobufSerde extends AbstractDeserializer { // implements SerDe {
  
    private StructTypeInfo rowTypeInfo;
    private ObjectInspector rowOI;
    private List<String> colNames;
    private HashSet<String> isNullableColLookup;
    private List<Object> row = new ArrayList<Object>();
    String clsMapping;
    Method newBuilderMethod;
    protected LoadFieldTuple resultType;
    boolean resultNullable = false;
    Configuration conf;
    
    
  //@Override
  public void initialize(final Configuration conf, final Properties tbl) throws SerDeException {
    try
    {
        String colNamesStr = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        colNames = Arrays.asList(colNamesStr.split(","));
        
        String colTypesStr = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        List<TypeInfo> colTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);
        
        rowTypeInfo = (StructTypeInfo)TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
        rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
        
        /*
        String fields = tbl.getProperty("fields");
        if(fields != null)
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
                throw new RuntimeException("Not supported yet: fields = " + fields);
            }
        }
        */
        
        String nullableColsString = tbl.getProperty("nullable");
        if(nullableColsString != null)
        {
            if(nullableColsString.equals("*"))
            {
                resultNullable = true;
            }
            else
            {
                isNullableColLookup = new HashSet<String>();
                for(String nullableColName : nullableColsString.split("\\s*,\\s*"))
                {
                    isNullableColLookup.add(nullableColName);
                }
            }
        }
        
        clsMapping = tbl.getProperty("class");
        if(clsMapping == null)
        {
            throw new SerDeException("class mapping expected: create ... with serdeproperties ( \"class\" = \"...\" )");
        }
        
        ensureResultType(conf);
    }
    catch(Throwable e)
    {
        e.printStackTrace();
        throw new RuntimeException("Error constructing Protobuf Deserializer: " + e.getMessage(), e);
    }
    
  }
  
  
  /*
  //@Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    throw new SerDeException("ProtobufSerde serialize not supported yet");
  }
  */


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
        /*
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
        */
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


    LoadFieldTuple repairMessageSchema(Message msg, StructTypeInfo parentTypeInfo)
    {
        return repairMessageSchema(null, msg.getDescriptorForType(), parentTypeInfo);
    }
    
    LoadFieldTuple repairMessageSchema(String[] parent, Descriptors.Descriptor desc, ListTypeInfo parentTypeInfo)
    {
        return repairMessageSchema(parent, desc, (StructTypeInfo)parentTypeInfo.getListElementTypeInfo());
    }
    
    LoadFieldTuple repairMessageSchema(String[] parent, Descriptors.Descriptor desc, StructTypeInfo parentTypeInfo)
    {
        LoadFieldTuple result = new LoadFieldTuple(parentTypeInfo);
        
        /*
            //Descriptors.Descriptor xdesc = msg.getDescriptorForType();
            Descriptors.Descriptor xdesc = desc;
            List<Descriptors.FieldDescriptor> xfields = xdesc.getFields();
            for(Descriptors.FieldDescriptor xfd : xfields)
            {
                //schema.add(new Schema.FieldSchema(xfd.getName(), DataType.UNKNOWN));
            }
        */
        
        List<TypeInfo> fieldTypes = parentTypeInfo.getAllStructFieldTypeInfos();
        List<String> fieldNames = parentTypeInfo.getAllStructFieldNames();
        int nfields = fieldTypes.size();
        for(int i = 0; i < nfields; i++)
        {
            TypeInfo f = fieldTypes.get(i);
            String name = fieldNames.get(i);
            boolean nullable = resultNullable || ( (isNullableColLookup != null) && isNullableColLookup.contains(name) );
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
                final Descriptors.FieldDescriptor fd = findFieldByNameIgnoreCase(curdesc, subProtoFieldPath[j]);
                if(fd == null)
                {
                    /*LoadField special = null;
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
                        //special.hiveTypeInfo = new Schema.FieldSchema(name, special.pigType);
                        special.name = name;
                        result.fields.add(special);
                        continue;
                    }
                    else*/
                    {
                        throw new RuntimeException("Cannot find field " + subProtoFieldPath[j] + " from " + java.util.Arrays.toString(protoFieldPath));
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
                        LoadFieldTuple lft;
                        if(fd.isRepeated())
                        {
                            lft = repairMessageSchema(protoFieldPath, vdesc, (ListTypeInfo)f);
                        }
                        else
                        {
                            lft = repairMessageSchema(protoFieldPath, vdesc, (StructTypeInfo)f);
                        }
                        lf = lft;
                        lf.hiveTypeInfo = f;
                        lf.name = name;
                    }
                    else
                    {
                        //final Object v = fd.getDefaultValue();
                        //if(v instanceof String)
                        if(ft == Descriptors.FieldDescriptor.JavaType.STRING)
                        {
                            lf = new LoadFieldScalar<String>();
                            lf.hiveTypeInfo = TypeInfoFactory.stringTypeInfo;
                        }
                        //else if(v instanceof Integer)
                        else if(ft == Descriptors.FieldDescriptor.JavaType.INT)
                        {
                            lf = new LoadFieldScalar<Integer>();
                            lf.hiveTypeInfo = TypeInfoFactory.intTypeInfo;
                        }
                        //else if(v instanceof Long)
                        else if(ft == Descriptors.FieldDescriptor.JavaType.LONG)
                        {
                            lf = new LoadFieldScalar<Long>();
                            lf.hiveTypeInfo = TypeInfoFactory.longTypeInfo;
                        }
                        //else if(v instanceof Float)
                        else if(ft == Descriptors.FieldDescriptor.JavaType.FLOAT)
                        {
                            lf = new LoadFieldScalar<Float>();
                            lf.hiveTypeInfo = TypeInfoFactory.floatTypeInfo;
                        }
                        //else if(v instanceof Double)
                        else if(ft == Descriptors.FieldDescriptor.JavaType.DOUBLE)
                        {
                            lf = new LoadFieldScalar<Double>();
                            lf.hiveTypeInfo = TypeInfoFactory.doubleTypeInfo;
                        }
                        //else if(v instanceof Boolean)
                        else if(ft == Descriptors.FieldDescriptor.JavaType.BOOLEAN)
                        {
                            if(TypeInfoFactory.booleanTypeInfo.equals(f))
                            {
                                lf = new LoadFieldScalar<Boolean>();
                                lf.hiveTypeInfo = TypeInfoFactory.booleanTypeInfo;
                            }
                            else
                            {
                                lf = new LoadFieldBooleanToInt();
                            }
                        }
                        //else if(v instanceof Descriptors.EnumValueDescriptor)
                        else if(ft == Descriptors.FieldDescriptor.JavaType.ENUM)
                        {
                            if(TypeInfoFactory.intTypeInfo.equals(f))
                            {
                                lf = new LoadFieldEnumToInt();
                                lf.hiveTypeInfo = TypeInfoFactory.intTypeInfo;
                            }
                            else
                            {
                                lf = new LoadFieldEnum();
                            }
                        }
                        else if(ft == Descriptors.FieldDescriptor.JavaType.BYTE_STRING)
                        {
                            if(TypeInfoFactory.stringTypeInfo.equals(f))
                            {
                                lf = new LoadFieldBytesToString();
                                lf.hiveTypeInfo = TypeInfoFactory.stringTypeInfo;
                            }
                            else
                            {
                                lf = new LoadFieldBytes();
                            }
                        }
                        if(lf != null)
                        {
                            lf.name = name;
                        }
                        if(fd.isRepeated())
                        {
                            throw new RuntimeException("array of non-structs not supported yet");
                        }
                    }
                    if(lf == null)
                    {
                        //throw new RuntimeException("Unhandled type found in protobuf: " + name + " of type: " + v.getClass().getName());
                        throw new RuntimeException("Unhandled type found in protobuf: " + name);
                    }
                    lf.nullable = nullable;
                    lf.protoFieldPath = protoFieldPath;
                    result.fields.add(lf);
                    break;
                }
            }
        }
        return result;
    }
    
    
    public static Descriptors.FieldDescriptor findFieldByNameIgnoreCase(final Descriptors.Descriptor desc, final String name) {
        //return desc.findFieldByName(name); // Only if name is all lowercase.
        List<Descriptors.FieldDescriptor> fields = desc.getFields(); // Likely causes a few allocations. TODO: optimize?
        for(Descriptors.FieldDescriptor f : fields)
        {
            if(name.toLowerCase().equals(f.getName().toLowerCase()))
            {
                return f;
            }
        }
        return null;
    }


    static class LoadField<T>
    {
        TypeInfo hiveTypeInfo = TypeInfoFactory.unknownTypeInfo;
        String name;
        boolean nullable = false;
        
        String[] protoFieldPath; // Protobuf path.
        Descriptors.FieldDescriptor protoField; // isOptional, isRepeated, etc.
        //Descriptors.FieldDescriptor.Type protoFieldType; // Descriptors.FieldDescriptor.getType()
        
        LoadField()
        {
            //this.hiveTypeInfo = DataType.findType(T.class);
        }
        
        @Override
        public String toString()
        {
            if(hiveTypeInfo != null)
            {
                return super.toString() + "@" + hiveTypeInfo.toString();
            }
            return super.toString();
        }
        
        T getValue(Message msg)
        {
            return null;
        }
        
        T getValueRepeated(Message msg, int index)
        {
            // FIXME
            String xname = "N/A";
            if(this.name != null)
            {
                xname = this.name;
            }
            throw new RuntimeException("Repeated not implemented for " + xname);
            //return null;
        }
        
        // Descends into messages specified by protoFieldPath.
        static Message descend(Message msg, String[] protoFieldPath, int until)
        {
            if(protoFieldPath.length == 0)
            {
                throw new RuntimeException("Nothing to descend");
            }
            for(int i = 0; i < protoFieldPath.length + until; i++)
            {
                Descriptors.Descriptor desc = msg.getDescriptorForType();
                Descriptors.FieldDescriptor fd = findFieldByNameIgnoreCase(desc, protoFieldPath[i]);
                if(fd == null)
                {
                    throw new RuntimeException("Cannot descend into " + protoFieldPath[i] + " from " + java.util.Arrays.toString(protoFieldPath));
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
        
        Message descend(Message msg, int until)
        {
            return descend(msg, this.protoFieldPath, until);
        }
        
    }
    
    // Bag or tuple.
    static class LoadFieldNestable<T> extends LoadField<T>
    {
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
    
    // A bag is an array in hive.
    static class LoadFieldBag extends LoadFieldNestable<List<Object>>
    {
        List<Object> bag;
        
        LoadFieldBag(StructTypeInfo ti)
        {
            super();
            this.hiveTypeInfo = ti;
            this.bag = new ArrayList<Object>();
        }
        
        @Override
        List<Object> getValue(Message msg)
        {
            if(fields.size() != 1 || !(fields.get(0) instanceof LoadFieldTuple))
            {
                throw new RuntimeException("LoadFieldBag expects one field of type LoadFieldTuple");
            }
            this.bag.clear();
            List<Object> result = this.bag;
            LoadField element = fields.get(0);
            for(int i = 0;; i++)
            {
                Object x = element.getValueRepeated(msg, i);
                if(x == null) // BUG: non-tuple rows can have null values, will cause the loop to end early!
                {
                    break;
                }
                result.add(x);
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
        Map getValue(Message msg)
        {
            HashMap result = new HashMap();
            LoadFieldTuple tuple = bagTuple;
            for(int i = 0;; i++)
            {
                List<Object> row = tuple.getValueRepeated(msg, i);
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
                    throw new RuntimeException("Cannot convert bag to Map<"
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
    
    
    // For Hive, Tuple is a Struct.
    // Note: a tuple itself won't be nullable, but if it is marked as nullable, all its fields should be nullable.
    static class LoadFieldTuple extends LoadFieldNestable<List<Object>>
    {
        List<Object> tuple;
        boolean created = false; // deprecated?
        StructTypeInfo hiveStructTypeInfo;
        
        LoadFieldTuple(StructTypeInfo ti)
        {
            super();
            this.hiveTypeInfo = ti;
            this.hiveStructTypeInfo = ti;
            int nfields = ti.getAllStructFieldTypeInfos().size();
            this.tuple = new ArrayList<Object>(nfields);
            for(int i = 0; i < nfields; i++)
            {
                this.tuple.add(null);
            }
        }
        
        List<Object> _getValue(Message msg, int how)
        {
            List<TypeInfo> fieldTypes = this.hiveStructTypeInfo.getAllStructFieldTypeInfos();
            int nfields = fieldTypes.size();
            if(nfields != fields.size())
            {
                throw new RuntimeException("Field count mismatch (pigSchema != fields)");
            }
            List<Object> t = this.tuple;
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
                        Descriptors.FieldDescriptor fd = findFieldByNameIgnoreCase(desc, protoFieldPath[protoFieldPath.length - 1]);
                        if(fd == null)
                        {
                            throw new RuntimeException("Tuple not found: " + java.util.Arrays.toString(protoFieldPath));
                        }
                        Descriptors.FieldDescriptor.JavaType ft = fd.getJavaType();
                        if(ft != Descriptors.FieldDescriptor.JavaType.MESSAGE)
                        {
                            throw new RuntimeException("Not a tuple: " + java.util.Arrays.toString(protoFieldPath));
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
        List<Object> getValue(Message msg)
        {
            return _getValue(msg, -1);
        }
        
        @Override
        List<Object> getValueRepeated(Message msg, int index)
        {
            return _getValue(msg, index);
        }
    }
    
    static class LoadFieldScalar<T> extends LoadField<T>
    {
        // how -1 = get value or default value.
        // how -2 = get value or null.
        // how >= 0 = get repeated index, or null if at end.
        static <T> T getValue(Message msg, String[] protoFieldPath, int how)
        {
            Message parentmsg = descend(msg, protoFieldPath, -1);
            Descriptors.Descriptor parentdesc = parentmsg.getDescriptorForType();
            Descriptors.FieldDescriptor fd = findFieldByNameIgnoreCase(parentdesc, protoFieldPath[protoFieldPath.length - 1]);
            if(fd == null)
            {
                throw new RuntimeException("Field not found: " + java.util.Arrays.toString(protoFieldPath));
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
        T getValue(Message msg)
        {
            return LoadFieldScalar.<T>getValue(msg, this.protoFieldPath, this.nullable ? -2 : -1);
        }
        
        @Override
        T getValueRepeated(Message msg, int index)
        {
            return LoadFieldScalar.<T>getValue(msg, this.protoFieldPath, index);
        }
    }
    
    static class LoadFieldBooleanToInt extends LoadFieldScalar<Integer>
    {
        LoadFieldBooleanToInt()
        {
            super();
            this.hiveTypeInfo = TypeInfoFactory.intTypeInfo;
        }
        
        static Integer getValue(Message msg, String[] protoFieldPath, int how)
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
        Integer getValue(Message msg)
        {
            return getValue(msg, this.protoFieldPath, this.nullable ? -2 : -1);
        }
        
        @Override
        Integer getValueRepeated(Message msg, int index)
        {
            return getValue(msg, this.protoFieldPath, index);
        }
    }
    
    static class LoadFieldBytes extends LoadFieldScalar<ByteArrayRef>
    {
        LoadFieldBytes()
        {
            super();
            this.hiveTypeInfo = TypeInfoFactory.binaryTypeInfo;
        }
        
        static ByteArrayRef wrapByteString(byte[] x)
        {
            ByteArrayRef y = new ByteArrayRef();
            y.setData(x);
            return y;
        }
        
        static ByteString getValue(Message msg, String[] protoFieldPath, int how)
        {
            ByteString bs = LoadFieldScalar.<ByteString>getValue(msg, protoFieldPath, how);
            if(bs == null)
            {
                return null;
            }
            return bs;
        }
        
        @Override
        ByteArrayRef getValue(Message msg)
        {
            ByteString bs = getValue(msg, this.protoFieldPath, this.nullable ? -2 : -1);
            if(bs == null)
            {
                return null;
            }
            return wrapByteString(bs.toByteArray());
        }
        
        @Override
        ByteArrayRef getValueRepeated(Message msg, int index)
        {
            ByteString bs = getValue(msg, this.protoFieldPath, index);
            if(bs == null)
            {
                return null;
            }
            return wrapByteString(bs.toByteArray());
        }
    }
    
    static class LoadFieldBytesToString extends LoadFieldScalar<String>
    {
        LoadFieldBytesToString()
        {
            super();
            this.hiveTypeInfo = TypeInfoFactory.stringTypeInfo;
        }
        
        static String getValue(Message msg, String[] protoFieldPath, int how)
        {
            ByteString bs = LoadFieldBytes.getValue(msg, protoFieldPath, how);
            if(bs == null)
            {
                return null;
            }
            return bs.toStringUtf8();
        }
        
        @Override
        String getValue(Message msg)
        {
            return getValue(msg, this.protoFieldPath, this.nullable ? -2 : -1);
        }
        
        @Override
        String getValueRepeated(Message msg, int index)
        {
            return getValue(msg, this.protoFieldPath, index);
        }
    }
    
    static class LoadFieldEnum extends LoadFieldScalar<String>
    {
        LoadFieldEnum()
        {
            super();
            this.hiveTypeInfo = TypeInfoFactory.stringTypeInfo;
        }
        
        static Descriptors.EnumValueDescriptor getValueEnum(Message msg, String[] protoFieldPath, int how)
        {
            return LoadFieldScalar.<Descriptors.EnumValueDescriptor>getValue(msg, protoFieldPath, how);
        }
        
        static String getValue(Message msg, String[] protoFieldPath, int how)
        {
            Descriptors.EnumValueDescriptor vv = getValueEnum(msg, protoFieldPath, how);
            if(vv == null)
            {
                return null;
            }
            return vv.getName();
        }
        
        @Override
        String getValue(Message msg)
        {
            return getValue(msg, this.protoFieldPath, this.nullable ? -2 : -1);
        }
        
        @Override
        String getValueRepeated(Message msg, int index)
        {
            return getValue(msg, this.protoFieldPath, index);
        }
    }
    
    static class LoadFieldEnumToInt extends LoadFieldScalar<Integer>
    {
        LoadFieldEnumToInt()
        {
            super();
            this.hiveTypeInfo = TypeInfoFactory.intTypeInfo;
        }
        
        static Integer getValue(Message msg, String[] protoFieldPath, int how)
        {
            Descriptors.EnumValueDescriptor vv = LoadFieldEnum.getValueEnum(msg, protoFieldPath, how);
            if(vv == null)
            {
                return null;
            }
            return vv.getNumber();
        }
        
        @Override
        Integer getValue(Message msg)
        {
            return getValue(msg, this.protoFieldPath, this.nullable ? -2 : -1);
        }
        
        @Override
        Integer getValueRepeated(Message msg, int index)
        {
            return getValue(msg, this.protoFieldPath, index);
        }
    }
    
    
    /*
    // Note: not a static class.
    class SpecialField_year extends LoadField<String>
    {
        SpecialField_year()
        {
            this.hiveTypeInfo = TypeInfoFactory.stringTypeInfo;
        }
        @Override
        String getValue(Message msg)
        {
            //updateInputInfo();
            return cachedInputSplitDateY;
        }
    }
    class SpecialField_month extends LoadField<String>
    {
        SpecialField_month()
        {
            this.hiveTypeInfo = TypeInfoFactory.stringTypeInfo;
        }
        @Override
        String getValue(Message msg)
        {
            //updateInputInfo();
            return cachedInputSplitDateM;
        }
    }
    class SpecialField_day extends LoadField<String>
    {
        SpecialField_day()
        {
            this.hiveTypeInfo = TypeInfoFactory.stringTypeInfo;
        }
        @Override
        String getValue(Message msg)
        {
            //updateInputInfo();
            return cachedInputSplitDateD;
        }
    }
    class SpecialField_hour extends LoadField<String>
    {
        SpecialField_hour()
        {
            this.hiveTypeInfo = TypeInfoFactory.stringTypeInfo;
        }
        @Override
        String getValue(Message msg)
        {
            //updateInputInfo();
            return cachedInputSplitDateH;
        }
    }
    class SpecialField_inputFile extends LoadField<String>
    {
        SpecialField_inputFile()
        {
            this.hiveTypeInfo = TypeInfoFactory.stringTypeInfo;
        }
        @Override
        String getValue(Message msg)
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
            this.hiveTypeInfo = TypeInfoFactory.intTypeInfo;
        }
        @Override
        Integer getValue(Message msg)
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
            this.hiveTypeInfo = TypeInfoFactory.stringTypeInfo;
        }
        @Override
        String getValue(Message msg)
        {
            return msg.toString();
        }
    }
    class SpecialField_lineSize extends LoadField<Integer>
    {
        SpecialField_lineSize()
        {
            this.hiveTypeInfo = TypeInfoFactory.intTypeInfo;
        }
        @Override
        Integer getValue(Message msg)
        {
            return lineSize;
        }
    }
    class SpecialField_msgSize extends LoadField<Integer>
    {
        SpecialField_msgSize()
        {
            this.hiveTypeInfo = TypeInfoFactory.intTypeInfo;
        }
        @Override
        Integer getValue(Message msg)
        {
            return msgSize;
        }
    }
    class SpecialField_rawLine extends LoadField<String>
    {
        SpecialField_rawLine()
        {
            this.hiveTypeInfo = TypeInfoFactory.stringTypeInfo;
        }
        @Override
        String getValue(Message msg)
        {
            return lineText.toString();
        }
    }
    */


    void ensureResultType(Configuration conf)
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
            resultType = repairMessageSchema(msg, this.rowTypeInfo);
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

    
    protected List<Object> getTuple(Message msg) {
        return resultType.getValue(msg);
    }


  //@Override
  public Object deserialize(Writable inputBlob) throws SerDeException {
    Text value = (Text)inputBlob;
    
    boolean hasData = false;
    try {
        byte[] buf = value.getBytes();
        int len = value.getLength();
        lineSize = len;
        lineText = value;
        
        updateInputInfo();
        
        byte[] pbraw = Base64.decodeBase64(buf, len);
        msgSize = pbraw.length;
        Message.Builder builder = newBuilder();
        Message msg = builder.mergeFrom(pbraw).build();
        
        if(msg == null) {
            //ProtobufParseFail:_total").increment(1);
            //ProtobufParseFail:null").increment(1);
        } else {
            // Return result:
            return getTuple(msg);
        }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        //"ProtobufParseFail:_total").increment(1);
        //"ProtobufParseFail:" + e.getMessage()).increment(1);
    } catch(Throwable e) {
        //throw new SerDeException(e);
    }
    
    if(!hasData) {
        for(int i = 0; i < row.size(); i++) {
            row.set(i, null);
        }
    }
    
    return row;
  }
  

  //@Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return rowOI;
  }


/*
  //@Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }
*/

  
  //@Override
  public SerDeStats getSerDeStats() {
    return null;
  }
  
}
