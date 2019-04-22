package org.palituxd.kafkapoc.exchange;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;
import org.palituxd.kafkapoc.model.CustomObject;

import java.util.Map;

public class CustomDeserializer implements Deserializer<CustomObject> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public CustomObject deserialize(String topic, byte[] data) {
        CustomObject object = null;
        try {
            object = new Gson().fromJson(new String(data), CustomObject.class);
        } catch (Exception exception) {
            System.out.println("Error in deserializing bytes " + exception);
        }
        return object;
    }

    @Override
    public void close() {
    }
}