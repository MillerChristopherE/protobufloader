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
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.*;

import com.google.protobuf.Message;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ByteString;

import org.protobufloader.util.Base64;


public class ProtobufSerde2 extends AbstractDeserializer { // implements SerDe {
    
    ObjectInspector rowOI;
    Method newBuilderMethod;
    String clsMapping;
    //Configuration conf;
    boolean printedError = false;
    //boolean resultNullable = false;
    
    //@Override
    public void initialize(final Configuration conf, final Properties tbl) throws SerDeException {
        //this.conf = conf;
        
        clsMapping = tbl.getProperty("class");
        if(clsMapping == null)
        {
            throw new SerDeException("class mapping expected: create ... with serdeproperties ( \"class\" = \"...\" )");
        }
        
        String nullableColsString = tbl.getProperty("nullable");
        if(nullableColsString != null)
        {
            if(nullableColsString.equals("*"))
            {
                //resultNullable = true;
                throw new RuntimeException("Not supported yet: nullable = " + nullableColsString);
            }
            else if(!nullableColsString.equals(""))
            {
                throw new RuntimeException("Not supported yet: nullable = " + nullableColsString);
            }
        }
        
        try {
            Class<? extends Message> cls = (Class<? extends Message>)loadProtoClass(clsMapping, conf);
            newBuilderMethod = cls.getMethod("newBuilder", new Class[] {});
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        Message.Builder builder = newBuilder();
        Message msg = builder.getDefaultInstanceForType();
        rowOI = createRootObjectInspector(msg.getDescriptorForType());
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
    
    
    protected ObjectInspector createRootObjectInspector(Descriptors.Descriptor desc)
    {
        return new RootMessageInspector(desc);
    }
    
    
    static class MessageField implements StructField
    {
        IProtoObjectInspector oi;
        String name;
        
        public MessageField(IProtoObjectInspector oi, String name)
        {
            this.oi = oi;
            this.name = name;
        }
        
        public String getFieldName()
        {
            return this.name;
        }
        
        public IProtoObjectInspector getFieldObjectInspector()
        {
            return this.oi;
        }
        
        public String getFieldComment()
        {
            //return null;
            return oi.getProtoFieldDescriptor().toProto().toString().replaceAll("[\r\n]+", "; ");
        }
    }
    
    protected interface IProtoObjectInspector extends ObjectInspector
    {
        String getProtoFieldName();
        Descriptors.FieldDescriptor getProtoFieldDescriptor();
    }
    
    protected interface IProtoLazy
    {
        Object getProtoValue();
        int getProtoRepeatedCount();
        boolean getProtoIsNull();
        IProtoLazy createProtoClone(int index, boolean cached); // index=-1 if not repeated, -2 to use the same index.
    }
    
    static protected class ProtoLazyField implements IProtoLazy
    {
        protected Message parent;
        protected Descriptors.FieldDescriptor fieldDesc;
        protected int index;
        protected ProtoLazyField reusenext;
        
        private ProtoLazyField()
        {
        }
        
        public final Descriptors.FieldDescriptor getFieldDescriptor()
        {
            return fieldDesc;
        }
        
        public Object getProtoValue()
        {
            //if(parent == null) throw new RuntimeException("Found NULL parent for " + fieldDesc.getFullName());
            //if(parent == null) return null;
            if(index != -1) return parent.getRepeatedField(fieldDesc, index);
            return parent.getField(fieldDesc);
        }
        
        public int getProtoRepeatedCount()
        {
            //if(parent == null) return 0;
            return parent.getRepeatedFieldCount(fieldDesc);
        }
        
        public boolean getProtoIsNull()
        {
            //if(parent == null) return true;
            return !parent.hasField(fieldDesc);
        }
        
        public IProtoLazy createProtoClone(int index, boolean cached)
        {
            if(index == -2)
            {
                index = this.index;
            }
            return ProtoLazyField.get(parent, fieldDesc, index, cached);
        }
        
        
        static ProtoLazyField reusefirst = null;
        static ProtoLazyField reusecur = null;
        
        public static void reset()
        {
            if(reusefirst == null) reusefirst = new ProtoLazyField();
            reusecur = reusefirst;
        }
        
        public static ProtoLazyField get(Message parent, Descriptors.FieldDescriptor fieldDesc, int index, boolean cached)
        {
            if(parent == null)
            {
                return null;
            }
            ProtoLazyField plf;
            if(cached)
            {
                if(reusecur == null) throw new RuntimeException("End of all ProtoLazyField");
                if(reusecur.reusenext == null)
                {
                    reusecur.reusenext = new ProtoLazyField();
                }
                plf = reusecur.reusenext;
                reusecur = plf;
            }
            else
            {
                plf = new ProtoLazyField();
            }
            plf.parent = parent;
            plf.fieldDesc = fieldDesc;
            plf.index = index;
            return plf;
        }
        
        public static ProtoLazyField get(Message parent, Descriptors.FieldDescriptor fieldDesc, int index)
        {
            return get(parent, fieldDesc, index, true);
        }
        
        public static ProtoLazyField get(Message parent, Descriptors.FieldDescriptor fieldDesc)
        {
            return get(parent, fieldDesc, -1, true);
        }
        
    }
    
    static protected class ProtoLazyMap extends ProtoLazyField
    {
        Map map;
        MapFieldObjectInspector oi;
        
        Map getProtoMap()
        {
            //if(parent == null) return null;
            if(map == null)
            {
                map = new HashMap();
                int count = parent.getRepeatedFieldCount(fieldDesc);
                for(int index = 0; index < count; index++)
                {
                    Message kvpmsg = (Message)parent.getRepeatedField(fieldDesc, index);
                    Object key = kvpmsg.getField(oi.keyField.getProtoFieldDescriptor());
                    Object value = kvpmsg.getField(oi.valueField.getProtoFieldDescriptor());
                    map.put(key, value);
                }
            }
            return map;
        }
        
        @Override
        public Object getProtoValue()
        {
            return getProtoMap();
        }
        
        protected ProtoLazyMap(Message parent, MapFieldObjectInspector oi)
        {
            this.parent = parent;
            this.fieldDesc = oi.fieldDesc;
            this.oi = oi;
        }
    }
    
    static abstract class PrimitiveFieldObjectInspector
        implements PrimitiveObjectInspector, IProtoObjectInspector
    {
        Descriptors.FieldDescriptor fieldDesc;
        Descriptors.FieldDescriptor.Type fieldType;
        Descriptors.FieldDescriptor.JavaType fieldJavaType;
        String name;
        protected PrimitiveTypeInfo typeInfo;
        
        public String getProtoFieldName()
        {
            return this.name;
        }
        
        public Descriptors.FieldDescriptor getProtoFieldDescriptor()
        {
            return fieldDesc;
        }
        
        static class IntFieldObjectInspector extends PrimitiveFieldObjectInspector
            implements IntObjectInspector
        {
            IntFieldObjectInspector(Descriptors.FieldDescriptor fieldDesc)
                { super(fieldDesc, (PrimitiveTypeInfo)TypeInfoFactory.intTypeInfo); }
            public Class<Integer> getPrimitiveWritableClass()
                { return Integer.class; }
            public Integer getPrimitiveJavaObject(Object o)
                { return (Integer)super.getValue(o); }
            public IntWritable getPrimitiveWritableObject(Object o)
                { return new IntWritable(getPrimitiveJavaObject(o)); }
            public int get(Object o)
                { Integer v = getPrimitiveJavaObject(o); /*if(v == null) return 0;*/ return v.intValue(); }
        }
        
        static class LongFieldObjectInspector extends PrimitiveFieldObjectInspector
            implements LongObjectInspector
        {
            LongFieldObjectInspector(Descriptors.FieldDescriptor fieldDesc)
                { super(fieldDesc, (PrimitiveTypeInfo)TypeInfoFactory.longTypeInfo); }
            public Class<Long> getPrimitiveWritableClass()
                { return Long.class; }
            public Long getPrimitiveJavaObject(Object o)
                { return (Long)super.getValue(o); }
            public LongWritable getPrimitiveWritableObject(Object o)
                { return new LongWritable(getPrimitiveJavaObject(o)); }
            public long get(Object o)
                { Long v = getPrimitiveJavaObject(o); /*if(v == null) return 0L;*/ return v.longValue(); }
        }
        
        static class FloatFieldObjectInspector extends PrimitiveFieldObjectInspector
            implements FloatObjectInspector
        {
            FloatFieldObjectInspector(Descriptors.FieldDescriptor fieldDesc)
                { super(fieldDesc, (PrimitiveTypeInfo)TypeInfoFactory.floatTypeInfo); }
            public Class<Float> getPrimitiveWritableClass()
                { return Float.class; }
            public Float getPrimitiveJavaObject(Object o)
                { return (Float)super.getValue(o); }
            public FloatWritable getPrimitiveWritableObject(Object o)
                { return new FloatWritable(getPrimitiveJavaObject(o)); }
            public float get(Object o)
                { Float v = getPrimitiveJavaObject(o); /*if(v == null) return 0.0f;*/ return v.floatValue(); }
        }
        
        static class DoubleFieldObjectInspector extends PrimitiveFieldObjectInspector
            implements DoubleObjectInspector
        {
            DoubleFieldObjectInspector(Descriptors.FieldDescriptor fieldDesc)
                { super(fieldDesc, (PrimitiveTypeInfo)TypeInfoFactory.doubleTypeInfo); }
            public Class<Double> getPrimitiveWritableClass()
                { return Double.class; }
            public Double getPrimitiveJavaObject(Object o)
                { return (Double)super.getValue(o); }
            public DoubleWritable getPrimitiveWritableObject(Object o)
                { return new DoubleWritable(getPrimitiveJavaObject(o)); }
            public double get(Object o)
                { Double v = getPrimitiveJavaObject(o); /*if(v == null) return 0.0;*/ return v.doubleValue(); }
        }
        
        static class BooleanFieldObjectInspector extends PrimitiveFieldObjectInspector
            implements BooleanObjectInspector
        {
            BooleanFieldObjectInspector(Descriptors.FieldDescriptor fieldDesc)
                { super(fieldDesc, (PrimitiveTypeInfo)TypeInfoFactory.booleanTypeInfo); }
            public Class<Boolean> getPrimitiveWritableClass()
                { return Boolean.class; }
            public Boolean getPrimitiveJavaObject(Object o)
                { return (Boolean)super.getValue(o); }
            public BooleanWritable getPrimitiveWritableObject(Object o)
                { return new BooleanWritable(getPrimitiveJavaObject(o)); }
            public boolean get(Object o)
                { Boolean v  = getPrimitiveJavaObject(o); /*if(v == null) return false;*/ return v.booleanValue(); }
        }
        
        static class StringFieldObjectInspector extends PrimitiveFieldObjectInspector
            implements StringObjectInspector
        {
            StringFieldObjectInspector(Descriptors.FieldDescriptor fieldDesc)
                { super(fieldDesc, (PrimitiveTypeInfo)TypeInfoFactory.stringTypeInfo); }
            public String getPrimitiveJavaObject(Object o)
                { Object v = super.getValue(o); if(v == null) return null; return v.toString(); }
            public Text getPrimitiveWritableObject(Object o)
                { String v = getPrimitiveJavaObject(o); /*if(v == null) return null;*/ return new Text(v); }
        }
        
        static class EnumFieldObjectInspector extends PrimitiveFieldObjectInspector
            implements StringObjectInspector
        {
            EnumFieldObjectInspector(Descriptors.FieldDescriptor fieldDesc)
                { super(fieldDesc, (PrimitiveTypeInfo)TypeInfoFactory.stringTypeInfo); }
            @Override public String getValue(Object o)
                { if(o == null) return null; IProtoLazy pl = (IProtoLazy)o;
                    return ((Descriptors.EnumValueDescriptor)pl.getProtoValue()).getName(); }
            public String getPrimitiveJavaObject(Object o)
                { return getValue(o); }
            public Text getPrimitiveWritableObject(Object o)
                { String v = getPrimitiveJavaObject(o); /*if(v == null) return null;*/ return new Text(v); }
        }
        
        static class BytesFieldObjectInspector extends PrimitiveFieldObjectInspector
            implements BinaryObjectInspector
        {
            BytesFieldObjectInspector(Descriptors.FieldDescriptor fieldDesc)
                { super(fieldDesc, (PrimitiveTypeInfo)TypeInfoFactory.binaryTypeInfo); }
            @Override public byte[] getValue(Object o)
                { if(o == null) return null; IProtoLazy pl = (IProtoLazy)o;
                    return ((ByteString)pl.getProtoValue()).toByteArray(); }
            public byte[] getPrimitiveJavaObject(Object o)
                { return getValue(o); }
            public BytesWritable getPrimitiveWritableObject(Object o)
                { byte[] v = getPrimitiveJavaObject(o); /*if(v == null) return null;*/ return new BytesWritable(v); }
        }
        
        public static PrimitiveFieldObjectInspector create(Descriptors.FieldDescriptor fieldDesc)
        {
            Descriptors.FieldDescriptor.JavaType fieldJavaType = fieldDesc.getJavaType();
            if(fieldJavaType == Descriptors.FieldDescriptor.JavaType.INT)
            {
                return new IntFieldObjectInspector(fieldDesc);
            }
            else if(fieldJavaType == Descriptors.FieldDescriptor.JavaType.LONG)
            {
                return new LongFieldObjectInspector(fieldDesc);
            }
            else if(fieldJavaType == Descriptors.FieldDescriptor.JavaType.FLOAT)
            {
                return new FloatFieldObjectInspector(fieldDesc);
            }
            else if(fieldJavaType == Descriptors.FieldDescriptor.JavaType.DOUBLE)
            {
                return new DoubleFieldObjectInspector(fieldDesc);
            }
            else if(fieldJavaType == Descriptors.FieldDescriptor.JavaType.BOOLEAN)
            {
                return new BooleanFieldObjectInspector(fieldDesc);
            }
            else if(fieldJavaType == Descriptors.FieldDescriptor.JavaType.STRING)
            {
                return new StringFieldObjectInspector(fieldDesc);
            }
            else if(fieldJavaType == Descriptors.FieldDescriptor.JavaType.ENUM)
            {
                return new EnumFieldObjectInspector(fieldDesc);
            }
            else if(fieldJavaType == Descriptors.FieldDescriptor.JavaType.BYTE_STRING)
            {
                return new BytesFieldObjectInspector(fieldDesc);
            }
            else
            {
                throw new RuntimeException("Unknown type " + fieldDesc.getType().toString());
            }
        }
        
        private PrimitiveFieldObjectInspector(Descriptors.FieldDescriptor fieldDesc, PrimitiveTypeInfo typeInfo)
        {
            this.fieldDesc = fieldDesc;
            this.typeInfo = typeInfo;
            this.fieldType = fieldDesc.getType();
            this.fieldJavaType = fieldDesc.getJavaType();
            this.name = fieldDesc.getName().toLowerCase();
        }
        
        @Override
        public Class<?> getJavaPrimitiveClass()
        {
            return typeInfo.getPrimitiveJavaClass();
        }
        
        @Override
        public PrimitiveObjectInspector.PrimitiveCategory getPrimitiveCategory()
        {
            return typeInfo.getPrimitiveCategory();
        }
        
        @Override
        public Class<?> getPrimitiveWritableClass()
        {
            return typeInfo.getPrimitiveWritableClass();
        }
        
        @Override
        public Category getCategory()
        {
            return Category.PRIMITIVE;
        }
        
        @Override
        public String getTypeName()
        {
            return typeInfo.getTypeName();
        }
        
        public PrimitiveTypeInfo getTypeInfo()
        {
            return this.typeInfo;
        }
        
        /*
        @Override
        public int precision()
        {
            switch(typeInfo.getPrimitiveCategory())
            {
                case FLOAT: return 7;
                case DOUBLE: return 15;
                case BYTE: return 3;
                case SHORT: return 5;
                case INT: return 10;
                case LONG: return 19;
                case VOID: return 1;
                case BOOLEAN: return 2;
                //case DECIMAL: return ((DecimalTypeInfo)typeInfo).precision();
                default: return 38; // Max, fits into 128 bits.
            }
        }
        
        @Override
        public int scale()
        {
            switch (typeInfo.getPrimitiveCategory())
            {
                case FLOAT: return 7;
                case DOUBLE: return 15;
                case BYTE:
                case SHORT:
                case INT:
                case LONG:
                case VOID:
                case BOOLEAN:
                    return 0;
                //case DECIMAL: return ((DecimalTypeInfo)typeInfo).scale();
                default: return 38;
            }
        }
        */
        
        BaseTypeParams typeParams;
        public BaseTypeParams getTypeParams()
        {
            return typeParams;
        }
        public void setTypeParams(BaseTypeParams typeParams)
        {
            this.typeParams = typeParams;
        }
        
        // o will be type of the value, return a copy if immutable.
        public Object copyObject(Object o)
        {
            //return o;
            if(o == null) return null;
            IProtoLazy pl = (IProtoLazy)o;
            return pl.createProtoClone(-2, false);
        }

        @Override
        public boolean preferWritable()
        {
            return false;
        }
        
        public Object getValue(Object o)
        {
            //if(o == null) return null;
            IProtoLazy pl = (IProtoLazy)o;
            return pl.getProtoValue();
        }
        
        /*
        @Override
        public Object create(double value)
        {
            throw new RuntimeException("PrimitiveFieldObjectInspector create() not supported");
        }
        */
        
        /*
        @Override
        public Object set(Object o, double value)
        {
            throw new RuntimeException("PrimitiveFieldObjectInspector set() not supported");
        }
        */
        
    }
    
    
    static class RepeatedFieldObjectInspector implements ListObjectInspector, IProtoObjectInspector
    {
        Descriptors.FieldDescriptor fieldDesc;
        IProtoObjectInspector elementInspector;
        
        RepeatedFieldObjectInspector(Descriptors.FieldDescriptor fieldDesc, IProtoObjectInspector elementInspector)
        {
            this.fieldDesc = fieldDesc;
            this.elementInspector = elementInspector;
        }
        
        public ObjectInspector getListElementObjectInspector()
        {
            return elementInspector;
        }
        
        public Object getListElement(Object data, int index)
        {
            IProtoLazy pl = (IProtoLazy)data;
            return pl.createProtoClone(index, true);
        }

        public int getListLength(Object data)
        {
            IProtoLazy pl = (IProtoLazy)data;
            return pl.getProtoRepeatedCount();
        }
        
        public List<Object> getList(Object data)
        {
            IProtoLazy pl = (IProtoLazy)data;
            int count = pl.getProtoRepeatedCount();
            ArrayList<Object> results = new ArrayList<Object>(count);
            for(int index = 0; index < count; index++)
            {
                results.add(pl.createProtoClone(index, true));
            }
            return results;
        }
        
        public String getTypeName()
        {
            return "array<" + elementInspector.getTypeName() + ">";
        }
        
        public Category getCategory()
        {
            return Category.LIST;
        }
        
        
        public String getProtoFieldName()
        {
            return elementInspector.getProtoFieldName();
        }
        
        public Descriptors.FieldDescriptor getProtoFieldDescriptor()
        {
            return fieldDesc;
        }
    }
    
    
    static class MapFieldObjectInspector implements MapObjectInspector, IProtoObjectInspector
    {
        Descriptors.FieldDescriptor fieldDesc;
        MessageInspector kvpInspector;
        IProtoObjectInspector keyField, valueField; // Note: actual fields in the proto, not the data in the serde.
        // Data returned from the serde for key/value are the actual types, since it's fully loaded at first key lookup.
        ObjectInspector serdeKeyOI, serdeValueOI;
        
        public static MapFieldObjectInspector tryCreate(Descriptors.FieldDescriptor fieldDesc, IProtoObjectInspector testKvpInspector)
        {
            if(fieldDesc.isRepeated())
            {
                if(testKvpInspector instanceof MessageInspector)
                {
                    MessageInspector kvpInspector = (MessageInspector)testKvpInspector;
                    List<IProtoObjectInspector> fields = kvpInspector.getMessageFieldInspectors();
                    if(fields.size() == 2)
                    {
                        IProtoObjectInspector keyf = fields.get(0);
                        IProtoObjectInspector valf = fields.get(1);
                        if("key".equals(keyf.getProtoFieldName()) && "value".equals(valf.getProtoFieldName()))
                        {
                            if(keyf instanceof PrimitiveObjectInspector && valf instanceof PrimitiveObjectInspector)
                            {
                                return new MapFieldObjectInspector(fieldDesc, kvpInspector);
                            }
                        }
                    }
                }
            }
            return null;
        }
        
        MapFieldObjectInspector(Descriptors.FieldDescriptor fieldDesc, MessageInspector kvpInspector)
        {
            this.fieldDesc = fieldDesc;
            this.kvpInspector = kvpInspector;
            List<IProtoObjectInspector> fields = kvpInspector.getMessageFieldInspectors();
            this.keyField = fields.get(0);
            this.valueField = fields.get(1);
            this.serdeKeyOI = PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(
                ((PrimitiveObjectInspector)this.keyField).getJavaPrimitiveClass()
                );
            this.serdeValueOI = PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(
                ((PrimitiveObjectInspector)this.valueField).getJavaPrimitiveClass()
                );
        }
        
        public ObjectInspector getMapKeyObjectInspector()
        {
            //return keyField;
            return serdeKeyOI;
        }
        
        public ObjectInspector getMapValueObjectInspector()
        {
            //return valueField;
            return serdeValueOI;
        }
        
        public Map getMap(Object data)
        {
            //if(data == null) return null;
            ProtoLazyMap plm = (ProtoLazyMap)data;
            return plm.getProtoMap();
        }
        
        public Object getMapValueElement(Object data, Object key)
        {
            //if(data == null) return null;
            ProtoLazyMap plm = (ProtoLazyMap)data;
            return plm.getProtoMap().get(key);
        }
        
        public int getMapSize(Object data)
        {
            //if(data == null) return null;
            ProtoLazyMap plm = (ProtoLazyMap)data;
            return plm.getProtoMap().size();
        }
        
        public String getTypeName()
        {
            return "map<" + keyField.getTypeName() + "," + valueField.getTypeName() + ">";
        }
        
        public Category getCategory()
        {
            return Category.MAP;
        }
        
        public String getProtoFieldName()
        {
            return kvpInspector.getProtoFieldName();
        }
        
        public Descriptors.FieldDescriptor getProtoFieldDescriptor()
        {
            return fieldDesc;
        }
    }
    
    
    static class MessageInspector extends StructObjectInspector implements IProtoObjectInspector
    {
        Descriptors.FieldDescriptor fieldDesc; // null for root!
        Descriptors.Descriptor desc;
        ArrayList<IProtoObjectInspector> fields;
        String name; // null at root.
        
        public String getProtoFieldName()
        {
            return this.name;
        }
        
        public Descriptors.FieldDescriptor getProtoFieldDescriptor()
        {
            return fieldDesc;
        }
        
        protected MessageInspector(Descriptors.Descriptor desc)
        {
            this.desc = desc;
        }
        
        public MessageInspector(Descriptors.FieldDescriptor fieldDesc)
        {
            this.fieldDesc = fieldDesc;
            this.desc = fieldDesc.getMessageType();
            this.name = fieldDesc.getName().toLowerCase();
        }
        
        public Category getCategory()
        {
            return Category.STRUCT;
        }
        
        public String getTypeName()
        {
            //return desc.getName(); // the type's unqualified name
            ensureLoaded();
            StringBuilder sb = new StringBuilder();
            sb.append("struct<");
            for(int i = 0; i < this.fields.size(); i++)
            {
                IProtoObjectInspector foi = this.fields.get(i);
                if(i != 0)
                {
                    sb.append(',');
                }
                String fname = foi.getProtoFieldName();
                if(fname != null)
                {
                    sb.append(fname);
                    sb.append(':');
                }
                sb.append(foi.getTypeName());
            }
            sb.append('>');
            return sb.toString();
        }
        
        @Override
        public boolean isSettable()
        {
            return false;
        }
        
        protected IProtoObjectInspector createFieldObjectInspector(Descriptors.FieldDescriptor fd)
        {
            Descriptors.FieldDescriptor.JavaType ft = fd.getJavaType();
            IProtoObjectInspector oi;
            if(ft == Descriptors.FieldDescriptor.JavaType.MESSAGE)
            {
                Descriptors.Descriptor fdesc = fd.getMessageType();
                oi = new MessageInspector(fd);
            }
            else
            {
                oi = PrimitiveFieldObjectInspector.create(fd);
            }
            if(fd.isRepeated())
            {
                MapFieldObjectInspector mapoi = MapFieldObjectInspector.tryCreate(fd, oi);
                if(mapoi != null)
                {
                    oi = mapoi;
                }
                else
                {
                    oi = new RepeatedFieldObjectInspector(fd, oi);
                }
            }
            return oi;
        }
        
        protected void ensureLoaded()
        {
            if(null == fields)
            {
                List<Descriptors.FieldDescriptor> dfields = desc.getFields();
                fields = new ArrayList<IProtoObjectInspector>(dfields.size());
                for(int i = 0; i < dfields.size(); i++)
                {
                    Descriptors.FieldDescriptor fd = dfields.get(i);
                    IProtoObjectInspector foi = createFieldObjectInspector(fd);
                    fields.add(foi);
                }
            }
        }
        
        public List<IProtoObjectInspector> getMessageFieldInspectors()
        {
            ensureLoaded();
            return this.fields;
        }
        
        @Override
        public List<StructField> getAllStructFieldRefs()
        {
            ensureLoaded();
            // TODO: cache?
            ArrayList<StructField> results = new ArrayList<StructField>(fields.size());
            for(int i = 0; i < fields.size(); i++)
            {
                IProtoObjectInspector oi = fields.get(i);
                results.add(new MessageField(oi, oi.getProtoFieldName()));
            }
            return results;
        }
        
        @Override
        public MessageField getStructFieldRef(String fieldName)
        {
            ensureLoaded();
            for(int i = 0; i < fields.size(); i++)
            {
                IProtoObjectInspector foi = fields.get(i);
                if(fieldName.equals(foi.getProtoFieldName()))
                {
                    return new MessageField(foi, fieldName);
                }
            }
            return null;
        }
        
        private final ProtoLazyField getProtoLazyField(Message msgparent, IProtoObjectInspector foi)
        {
            if(foi.getCategory() == Category.MAP)
            {
                return new ProtoLazyMap(msgparent, (MapFieldObjectInspector)foi);
            }
            else
            {
                return ProtoLazyField.get(msgparent, foi.getProtoFieldDescriptor());
            }
        }
        
        @Override
        public Object getStructFieldData(Object data, StructField fieldRef)
        {
            if(data == null)
            {
                return null;
            }
            //ensureLoaded();
            IProtoLazy plfparent = (IProtoLazy)data;
            Message msgparent = (Message)plfparent.getProtoValue();
            IProtoObjectInspector foi = (IProtoObjectInspector)fieldRef.getFieldObjectInspector();
            return getProtoLazyField(msgparent, foi);
        }
        
        @Override
        public List<Object> getStructFieldsDataAsList(Object data)
        {
            if(data == null)
            {
                return null;
            }
            ensureLoaded();
            IProtoLazy plfparent = (IProtoLazy)data;
            Message msgparent = (Message)plfparent.getProtoValue();
            ArrayList<Object> results = new ArrayList<Object>(fields.size());
            for(int i = 0; i < fields.size(); i++)
            {
                IProtoObjectInspector foi = fields.get(i);
                results.add(getProtoLazyField(msgparent, foi));
            }
            return results;
        }
        
    }
    
    
    static class RootMessageInspector extends MessageInspector
    {
        public RootMessageInspector(Descriptors.Descriptor desc)
        {
            super(desc);
        }
        
        /*
        protected void ensureLoaded()
        {
            super.ensureLoaded();
        }
        */
        
    }
    
    
    static class ProtoRoot implements IProtoLazy
    {
        Message msg;
        
        public Object getProtoValue()
        {
            return msg;
        }
        public int getProtoRepeatedCount()
        {
            throw new IllegalArgumentException("Record itself cannot be repeated");
        }
        
        public boolean getProtoIsNull()
        {
            return getProtoValue() == null;
        }
        
        public IProtoLazy createProtoClone(int index, boolean cached)
        {
            if(index >= 0)
            {
                throw new IllegalArgumentException("Record itself cannot be repeated");
            }
            ProtoRoot clone = new ProtoRoot();
            clone.msg = this.msg;
            return clone;
        }
    }
    
    
    // This doesn't work because Hive wants a null object if the value is null,
    // and we won't know if it's a corrupt object (null) if parsed lazily.
    static class ProtoLazyRoot extends ProtoRoot
    {
        public byte[] buffer;
        public int length = -1;
        public ProtobufSerde2 serde;
        Message msg;
        
        @Override
        public Object getProtoValue()
        {
            if(length != -1)
            {
                try
                {
                    msg = serde.getMessageFromBytes(buffer, length);
                }
                catch(RuntimeException re)
                {
                    throw re;
                }
                catch(Exception e)
                {
                    throw new RuntimeException(e);
                }
                buffer = null;
                length = -1;
            }
            return msg;
        }
        
        @Override
        public IProtoLazy createProtoClone(int index, boolean cached)
        {
            this.getProtoValue(); // Make sure this object is constructed.
            return super.createProtoClone(index, cached);
        }
        
    }
    
    
    protected Message getMessageFromBytes(byte[] inputBytes, int len) throws SerDeException, java.io.IOException {
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
        
        //msgSize = pbraw.length;
        Message.Builder builder = newBuilder();
        try
        {
            Message msg = builder.mergeFrom(pbraw).build();
            
            if(msg == null) {
                //ProtobufParseFail:_total").increment(1);
                //ProtobufParseFail:null").increment(1);
            } else {
                // Return result:
                return msg;
            }
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            /* // TODO: get these counters working in hive.
            PigStatusReporter rep = PigStatusReporter.getInstance();
            rep.getCounter("ProtobufLoadFields", "ProtobufParseFail:_total").increment(1);
            rep.getCounter("ProtobufLoadFields", "ProtobufParseFail:" + e.getMessage()).increment(1);
            */
        } catch (Throwable e) {
            if (!printedError) {
                e.printStackTrace();
                printedError = true;
            }
            /* // TODO: get these counters working in hive.
            PigStatusReporter rep = PigStatusReporter.getInstance();
            rep.getCounter("ProtobufLoadFields", "throw:" + e.getClass().getName()).increment(1);
            */
        }
        return null;
    }
    
    
    protected Object getRecordFromInput(byte[] inputBytes, int len) throws SerDeException, java.io.IOException
    {
        ProtoLazyField.reset();
        
        /*
        ProtoLazyRoot root = new ProtoLazyRoot();
        root.buffer = inputBytes;
        root.length = len;
        root.serde = this;
        */
        
        Message msg = getMessageFromBytes(inputBytes, len);
        if(msg == null)
        {
            return null;
        }
        ProtoRoot root = new ProtoRoot();
        root.msg = msg;
        
        return root;
    }


    //@Override
    public Object deserialize(Writable inputBlob) throws SerDeException {
        Text value = (Text)inputBlob;
        
        boolean hasData = false;
        try {
            byte[] wholebuf = value.getBytes();
            int len = value.getLength();
            //lineSize = len;
            //lineText = value;
            
            //updateInputInfo();
            
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
