package org.palituxd.twitterkafkapoc.producer;

import com.google.gson.Gson;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.palituxd.kafkapoc.KafkaConstants;
import org.palituxd.twitterkafkapoc.TwitterConstants;
import org.palituxd.twitterkafkapoc.exchange.TweetDeserializer;
import org.palituxd.twitterkafkapoc.exchange.TweetSerializer;
import org.palituxd.twitterkafkapoc.model.Tweet;
import org.palituxd.twitterkafkapoc.producer.callback.BasicCallback;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterKafkaProducer {
    private Client client;
    private BlockingQueue<String> queue;
    private Gson gson;
    private Callback callback;

    public TwitterKafkaProducer() {
        // Configure auth
        Authentication authentication = new OAuth1(
                TwitterConstants.CONSUMER_KEY,
                TwitterConstants.CONSUMER_SECRET,
                TwitterConstants.ACCESS_TOKEN,
                TwitterConstants.TOKEN_SECRET);

        // track the terms of your choice. here im only tracking #bigdata.
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        //endpoint.followings(Collections.singletonList(405100888l));
        endpoint.trackTerms(Collections.singletonList(TwitterConstants.HASHTAG));

        queue = new LinkedBlockingQueue<>(10000);

        client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .authentication(authentication)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(queue))
                .build();
        gson = new Gson();
        callback = new BasicCallback();
    }

    private Producer<Long, Tweet> getProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.KAFKA_BROKERS);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.KAFKA_BROKERS);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConstants.CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TweetSerializer.class.getName());
        return new KafkaProducer(properties);
    }

    public void run() {
        client.connect();
        try (Producer<Long, Tweet> producer = getProducer()) {
            TweetDeserializer deserializer = new TweetDeserializer();
            while (!client.isDone()) {
                String msg = queue.take();
                Tweet tweet = deserializer.deserialize(KafkaConstants.TOPIC_NAME, msg.getBytes());
                ProducerRecord<Long, Tweet> record = new ProducerRecord<>(KafkaConstants.TOPIC_NAME, tweet.getId(), tweet);
                producer.send(record, callback);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.stop();
        }
    }
}