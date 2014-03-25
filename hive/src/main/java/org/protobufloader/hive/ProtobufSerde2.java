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


// Input is line based text format, each line is base64 encoded protobuf message.
public class ProtobufSerde2 extends ProtobufSerde3 {
   
    @Override
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
    
    
    protected Message getMessageFromBase64Bytes(byte[] inputBytes, int len) throws SerDeException, java.io.IOException {
        byte[] pbraw = Base64.decodeBase64(inputBytes, len);
        return getMessageFromProtoBytes(pbraw, pbraw.length);
    }
    
    
    @Override
    protected Message getMessageFromInputBytes(byte[] inputBytes, int len) throws SerDeException, java.io.IOException {
        return getMessageFromBase64Bytes(inputBytes, len);
    }
    
}
