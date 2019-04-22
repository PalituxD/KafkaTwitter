package org.palituxd.kafkapoc.exchange;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;
import org.palituxd.kafkapoc.model.CustomObject;

import java.util.Map;

public class CustomSerializer implements Serializer<CustomObject> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, CustomObject data) {
        byte[] retVal = null;
        try {
            retVal = new Gson().toJson(data).getBytes();
        } catch (Exception exception) {
            System.out.println("Error in serializing object" + data);
        }
        return retVal;
    }

    @Override
    public void close() {
    }
}