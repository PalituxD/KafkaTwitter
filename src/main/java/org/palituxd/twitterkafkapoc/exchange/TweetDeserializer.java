package org.palituxd.twitterkafkapoc.exchange;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;
import org.palituxd.twitterkafkapoc.model.Tweet;

import java.util.Map;

public class TweetDeserializer  implements Deserializer<Tweet> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Tweet deserialize(String topic, byte[] data) {
        Tweet object = null;
        try {
            object = new Gson().fromJson(new String(data), Tweet.class);
        } catch (Exception exception) {
            System.out.println("Error in deserializing bytes " + exception);
        }
        return object;
    }

    @Override
    public void close() {
    }
}