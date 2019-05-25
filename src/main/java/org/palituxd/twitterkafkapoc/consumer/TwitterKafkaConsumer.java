package org.palituxd.twitterkafkapoc.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.bson.Document;
import org.palituxd.kafkapoc.KafkaConstants;
import org.palituxd.twitterkafkapoc.TwitterConstants;
import org.palituxd.twitterkafkapoc.exchange.TweetDeserializer;
import org.palituxd.twitterkafkapoc.model.Tweet;
import org.palituxd.twitterkafkapoc.services.StoreService;

import java.time.Duration;
import java.util.Calendar;
import java.util.Collections;
import java.util.Properties;

public class TwitterKafkaConsumer {

    public void run() {
        Consumer<Long, Tweet> consumer = null;
        try {
            consumer = getConsumer();
            int noMessageFound = 0;
            while (true) {
                System.out.println("Waiting " + noMessageFound + " for:" + Calendar.getInstance().getTimeInMillis());
                ConsumerRecords<Long, Tweet> consumerRecords = consumer.poll(Duration.ofSeconds(1l));
                if (consumerRecords.count() == 0) {
                    noMessageFound++;
                    if (noMessageFound > KafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                        break;
                    else
                        continue;
                }
                manageConsumerRecords(consumerRecords);
                consumer.commitAsync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    private void manageConsumerRecords(ConsumerRecords<Long, Tweet> consumerRecords){
        consumerRecords.forEach(record -> {
            print(record);
            Tweet tweet = record.value();
            if (tweet.getId() > 0) {
                saveTweet(tweet);
            }
        });
    }

    private void print(ConsumerRecord record){
        System.out.println("Record Key " + record.key());
        System.out.println("Record value " + record.value());
        System.out.println("Record partition " + record.partition());
        System.out.println("Record offset " + record.offset());
    }

    private void saveTweet(Tweet tweet){
        Document document = new Document();
        document.put("id", tweet.getId());
        document.put("userId", tweet.getUser().getId());
        document.put("userName", tweet.getUser().getName());
        document.put("tweet", tweet.getText());
        document.put("lang", tweet.getLang());
        document.put("hashtag", TwitterConstants.HASHTAG);
        StoreService.getInstance().insertOne(document);
    }

    private Consumer<Long, Tweet> getConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstants.GROUP_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TweetDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, KafkaConstants.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConstants.OFFSET_RESET_EARLIER);
        Consumer<Long, Tweet> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(KafkaConstants.TOPIC_NAME));
        return consumer;
    }
}