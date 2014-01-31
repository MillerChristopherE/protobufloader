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

import org.apache.hadoop.hive.serde2.lazy.*;

import com.google.protobuf.Message;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ByteString;

import org.protobufloader.util.Base64;


public class ProtobufLazySerdeTest extends ProtobufSerde {

    @Override
    public void initialize(final Configuration conf, final Properties tbl) throws SerDeException {
        super.initialize(conf, tbl);
    }
    
    private final Object _super_getRecordFromInput(byte[] inputBytes, int len) throws SerDeException, java.io.IOException {
        return super.getRecordFromInput(inputBytes, len);
    }
    
    @Override
    protected Object getRecord(Message msg) throws SerDeException, java.io.IOException {
        rootLazyStruct.protobufmsg = msg;
        return rootLazyStruct;
    }
    
    @Override
    protected Object getRecordFromInput(byte[] inputBytes, int len) throws SerDeException, java.io.IOException {
        return new RootProtobufLazyStruct(this.resultType, inputBytes, len);
    }
    
    RootProtobufLazyStruct rootLazyStruct;
    
    
    class ProtobufLazyStruct extends LazyStruct
    {
        LoadFieldStruct fieldStruct;
        Message protobufmsg;
        
        ProtobufLazyStruct(LoadFieldStruct fieldStruct, Message protobufmsg)
        {
            super(null); // ObjectInspector
            this.fieldStruct = fieldStruct;
            this.protobufmsg = protobufmsg;
            //setParsed(false);
        }
        
        void forceParse()
        {
        }
        
        private final void _ensureParsed()
        {
            if(!getParsed())
            {
                setParsed(true);
                forceParse();
            }
        }
        
        @Override
        public Object getField(int i)
        {
            final int how = -1;
            _ensureParsed();
            Message msg = fieldStruct.getRealMessage(protobufmsg, how);
            if(msg == null)
            {
                return null;
            }
            if(how < 0 && fieldStruct.fields.get(i) instanceof LoadFieldStruct)
            {
                return new ProtobufLazyStruct((LoadFieldStruct)fieldStruct.fields.get(i), msg);
            }
            else
            {
                return fieldStruct.getFieldValueFromRealMessage(msg, i, how);
            }
        }
        
        @Override
        public ArrayList<Object> getFieldsAsList()
        {
            final int how = -1;
            _ensureParsed();
            Message msg = fieldStruct.getRealMessage(protobufmsg, how);
            if(msg == null)
            {
                return null;
            }
            int nfields = fieldStruct.fields.size();
            ArrayList<Object> t = new ArrayList(nfields);
            for(int i = 0; i < nfields; i++)
            {
                if(how < 0 && fieldStruct.fields.get(i) instanceof LoadFieldStruct)
                {
                    t.add(new ProtobufLazyStruct((LoadFieldStruct)fieldStruct.fields.get(i), msg));
                }
                else
                {
                    t.add(fieldStruct.getFieldValueFromRealMessage(msg, i, how));
                }
            }
            return t;
        }

        @Override
        public Object getObject()
        {
            return this;
        }

        @Override
        protected LazyObject[] getFields() {
            throw new RuntimeException("LazyStruct.getFields()");
        }

        @Override
        public long getRawDataSerializedSize() {
            _ensureParsed();
            return protobufmsg.getSerializedSize();
        }
        
    }
    
    
    class RootProtobufLazyStruct extends ProtobufLazyStruct
    {
        byte[] inputBytes;
        int len;
        
        RootProtobufLazyStruct(LoadFieldStruct fieldStruct, byte[] inputBytes, int len)
        {
            super(fieldStruct, null);
            this.inputBytes = inputBytes;
            this.len = len;
        }
        
        @Override
        void forceParse()
        {
            try
            {
                rootLazyStruct = this;
                _super_getRecordFromInput(this.inputBytes, this.len); // Triggers getRecord which sets this.protobufmsg.
            }
            catch(RuntimeException re)
            {
                throw re;
            }
            catch(Exception e)
            {
                throw new RuntimeException(e);
            }
        }
        
    }
  
}
