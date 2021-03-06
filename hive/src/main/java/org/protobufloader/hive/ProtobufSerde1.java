package org.protobufloader.hive;

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
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.protobuf.Message;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ByteString;

import org.protobufloader.util.Base64;


public class ProtobufSerde1 extends AbstractDeserializer { // implements SerDe {
  
    private StructTypeInfo rowTypeInfo;
    private ObjectInspector rowOI;
    private List<String> colNames;
    private HashSet<String> isNullableColLookup;
    String clsMapping;
    Method newBuilderMethod;
    protected LoadFieldStruct resultType;
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


    LoadFieldStruct repairMessageSchema(Message msg, StructTypeInfo parentTypeInfo)
    {
        return repairMessageSchema(null, msg.getDescriptorForType(), parentTypeInfo);
    }
    
    LoadFieldStruct repairMessageSchema(String[] parent, Descriptors.Descriptor desc, ListTypeInfo parentTypeInfo)
    {
        return repairMessageSchema(parent, desc, (StructTypeInfo)parentTypeInfo.getListElementTypeInfo());
    }
    
    LoadFieldStruct repairMessageSchema(String[] parent, Descriptors.Descriptor desc, StructTypeInfo parentTypeInfo)
    {
        LoadFieldStruct result = new LoadFieldStruct(parentTypeInfo);
        
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
                        LoadFieldStruct lft;
                        if(fd.isRepeated())
                        {
                            if(f.getCategory().equals(ObjectInspector.Category.MAP))
                            {
                                // Let's make a ListTypeInfo wrapping a StructTypeInfo for schema lookup.
                                MapTypeInfo mti = (MapTypeInfo)f;
                                List<String> names = new ArrayList<String>();
                                names.add("key"); // Note: only works if they are literally called this in the proto.
                                names.add("value");
                                List<TypeInfo> types = new ArrayList<TypeInfo>();
                                types.add(mti.getMapKeyTypeInfo());
                                types.add(mti.getMapValueTypeInfo());
                                TypeInfo elementti = TypeInfoFactory.getStructTypeInfo(names, types);
                                ListTypeInfo lti = (ListTypeInfo)TypeInfoFactory.getListTypeInfo(elementti);
                                lft = repairMessageSchema(protoFieldPath, vdesc, lti);
                            }
                            else
                            {
                                lft = repairMessageSchema(protoFieldPath, vdesc, (ListTypeInfo)f);
                            }
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
                        /*if(fd.isRepeated())
                        {
                            throw new RuntimeException("array of non-structs not supported yet");
                        }*/
                    }
                    if(lf == null)
                    {
                        //throw new RuntimeException("Unhandled type found in protobuf: " + name + " of type: " + v.getClass().getName());
                        throw new RuntimeException("Unhandled type found in protobuf: " + name);
                    }
                    lf.nullable = nullable;
                    lf.protoFieldPath = protoFieldPath;
                    if(fd.isRepeated())
                    {
                        LoadFieldArray lfbag = new LoadFieldArray();
                        LoadField lfelement = lf;
                        lfbag.fields.add(lfelement);
                        lfbag.hiveTypeInfo = TypeInfoFactory.getListTypeInfo(lfelement.hiveTypeInfo);
                        lf = lfbag;
                        if(f.getCategory().equals(ObjectInspector.Category.MAP))
                        {
                            LoadFieldArrayToMap lfmap = new LoadFieldArrayToMap(lfbag);
                            lf = lfmap;
                        }
                    }
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


    static class LoadField<T> implements org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
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
        
        // ObjectInspector
        public String getTypeName()
        {
            return hiveTypeInfo.getTypeName();
        }
        
        // ObjectInspector
        public org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category getCategory()
        {
            return hiveTypeInfo.getCategory();
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
            String xname = "N/A";
            if(this.name != null)
            {
                xname = this.name;
            }
            throw new RuntimeException("Value not implemented for " + xname);
            //return null;
        }
        
        // Null return indicates the end of the repeated field.
        // This is valid because protobuf does not allow null entries in repeated fields.
        T getValueRepeated(Message msg, int index)
        {
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
    
    static class LoadFieldArray extends LoadFieldNestable<List<Object>>
    {
        LoadFieldArray()
        {
            super();
            //this.hiveTypeInfo = ti;
            this.hiveTypeInfo = TypeInfoFactory.unknownTypeInfo;
        }
        
        @Override
        List<Object> getValue(Message msg)
        {
            if(fields.size() != 1)
            {
                throw new RuntimeException("LoadFieldArray expects 1 field, not " + fields.size());
            }
            List<Object> result = new ArrayList<Object>();
            LoadField element = fields.get(0);
            for(int i = 0;; i++)
            {
                Object x = element.getValueRepeated(msg, i);
                if(x == null)
                {
                    break;
                }
                result.add(x);
            }
            return result;
        }
        
    }
    
    
    static class LoadFieldArrayToMap extends LoadField<Map>
    {
        LoadField arrayField;
        LoadFieldStruct arrayFieldIsStruct = null;
        boolean mapValueIsImplicitCount = false;
        
        public LoadFieldArrayToMap(LoadFieldArray lfarray)
        {
            this.name = lfarray.name;
            this.nullable = lfarray.nullable;
            if(lfarray.fields.size() != 1 || !(lfarray.fields.get(0) instanceof LoadFieldStruct))
            {
                throw new RuntimeException("LoadFieldArrayToMap bag expects one field");
            }
            this.arrayField = lfarray.fields.get(0);
            if(this.arrayField instanceof LoadFieldStruct)
            {
                LoadFieldStruct tuple = (LoadFieldStruct)this.arrayField;
                this.arrayFieldIsStruct = tuple;
                int nfields = tuple.fields.size();
                if(nfields == 0 || nfields > 2)
                {
                    throw new RuntimeException("Map needs 1 or 2 fields for key=value");
                }
                if(nfields == 1)
                {
                    this.hiveTypeInfo = TypeInfoFactory.getMapTypeInfo(tuple.fields.get(0).hiveTypeInfo, TypeInfoFactory.longTypeInfo);
                    this.mapValueIsImplicitCount = true;
                }
                else
                {
                    this.hiveTypeInfo = TypeInfoFactory.getMapTypeInfo(tuple.fields.get(0).hiveTypeInfo,
                        tuple.fields.get(1).hiveTypeInfo);
                }
            }
            else
            {
                this.hiveTypeInfo = TypeInfoFactory.getMapTypeInfo(this.arrayField.hiveTypeInfo, TypeInfoFactory.longTypeInfo);
                this.mapValueIsImplicitCount = true;
            }
        }
        
        @Override
        Map getValue(Message msg)
        {
            HashMap result = new HashMap();
            for(int i = 0;; i++)
            {
                Object k, v;
                LoadFieldStruct tuple = this.arrayFieldIsStruct;
                if(tuple != null)
                {
                    List<Object> row = tuple.getValueRepeated(msg, i);
                    if(row == null)
                    {
                        break;
                    }
                    k = row.get(0);
                    if(this.mapValueIsImplicitCount)
                    {
                        v = (Long)1L;
                    }
                    else
                    {
                        v = row.get(1);
                    }
                }
                else
                {
                    LoadField field = this.arrayField;
                    Object row = field.getValueRepeated(msg, i);
                    if(row == null)
                    {
                        break;
                    }
                    k = row;
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
                    if(oldv != null)
                    {
                        throw new RuntimeException("Cannot convert array to Map<"
                            + k.getClass().getName()
                            + ", "
                            + v.getClass().getName()
                            + "> - duplicate keys found"
                            );
                    }
                }
                result.put(k, v);
            }
            return result;
        }
    }
    
    
    // Note: if a struct is marked as nullable, all its fields should be nullable.
    static class LoadFieldStruct extends LoadFieldNestable<List<Object>>
    {
        boolean created = false; // deprecated?
        StructTypeInfo hiveStructTypeInfo;
        
        LoadFieldStruct(StructTypeInfo ti)
        {
            super();
            this.hiveTypeInfo = ti;
            this.hiveStructTypeInfo = ti;
            //int nfields = ti.getAllStructFieldTypeInfos().size();
        }
        
        // how is -1 for not repeated, or the repeated index.
        // Returns null if no such repeated index.
        protected Message getRealMessage(Message msg, int how)
        {
            if(created || protoFieldPath == null || how < 0)
            {
                return descend(msg, how);
            }
            else
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
                return (Message)msg.getRepeatedField(fd, how);
            }
        }
        
        protected Object getFieldValueFromRealMessage(Message realmsg, int fieldIndex, int how)
        {
            LoadField lf = fields.get(fieldIndex);
            lf.nullable = lf.nullable || this.nullable; // Propagate nullable.
            if(how >= 0)
            {
                return lf.getValueRepeated(realmsg, how);
            }
            else
            {
                return lf.getValue(realmsg);
            }
        }
        
        List<Object> _getValue(Message msg, int how)
        {
            List<TypeInfo> fieldTypes = this.hiveStructTypeInfo.getAllStructFieldTypeInfos();
            int nfields = fieldTypes.size();
            if(nfields != fields.size())
            {
                throw new RuntimeException("Field count mismatch (hive struct fields != internal fields)");
            }
            msg = getRealMessage(msg, how);
            if(msg == null)
            {
                return null;
            }
            List<Object> t = new ArrayList<Object>(nfields);
            for(int i = 0; i < nfields; i++)
            {
                t.add(getFieldValueFromRealMessage(msg, i, how));
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
    
    static class LoadFieldBytes extends LoadFieldScalar<byte[]>
    {
        LoadFieldBytes()
        {
            super();
            this.hiveTypeInfo = TypeInfoFactory.binaryTypeInfo;
        }
        
        static byte[] wrapByteString(byte[] x)
        {
            return x;
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
        byte[] getValue(Message msg)
        {
            ByteString bs = getValue(msg, this.protoFieldPath, this.nullable ? -2 : -1);
            if(bs == null)
            {
                return null;
            }
            return wrapByteString(bs.toByteArray());
        }
        
        @Override
        byte[] getValueRepeated(Message msg, int index)
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
    
    
    protected Object getRecord(Message msg) throws SerDeException, java.io.IOException {
        return resultType.getValue(msg);
    }
    
    
    protected Object getRecordFromInput(byte[] inputBytes, int len) throws SerDeException, java.io.IOException {
        byte[] pbraw = Base64.decodeBase64(inputBytes, len);
        /*
        byte[] goodbuf = inputBytes;
        if(goodbuf.length != len)
        {
            goodbuf = new byte[len];
            System.arraycopy(wholebuf, 0, goodbuf, 0, len);
        }
        byte[] pbraw = Base64.decodeBase64(goodbuf);
        */
        
        msgSize = pbraw.length;
        Message.Builder builder = newBuilder();
        Message msg = builder.mergeFrom(pbraw).build();
        
        if(msg == null) {
            //ProtobufParseFail:_total").increment(1);
            //ProtobufParseFail:null").increment(1);
        } else {
            // Return result:
            return getRecord(msg);
        }
        return null;
    }


  //@Override
  public Object deserialize(Writable inputBlob) throws SerDeException {
    Text value = (Text)inputBlob;
    
    boolean hasData = false;
    try {
        byte[] wholebuf = value.getBytes();
        int len = value.getLength();
        lineSize = len;
        lineText = value;
        
        updateInputInfo();
        
        return getRecordFromInput(wholebuf, len);
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        //"ProtobufParseFail:_total").increment(1);
        //"ProtobufParseFail:" + e.getMessage()).increment(1);
    } catch(Throwable e) {
        //throw new SerDeException(e);
    }
    return null;
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
