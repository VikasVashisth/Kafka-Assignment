package com.knoldus.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Map;

    /*A serialize class */

public class UserSerializer implements Serializer {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, Object o) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(o).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }



    @Override
    public void close() {

    }
}
