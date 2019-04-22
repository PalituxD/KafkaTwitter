package org.palituxd.twitterkafkapoc.consumer;

import com.mongodb.client.MongoCollection;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.bson.Document;
import org.palituxd.kafkapoc.KafkaConstants;
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
                // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
                if (consumerRecords.count() == 0) {
                    noMessageFound++;
                    if (noMessageFound > KafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                        // If no message found count is reached to threshold exit loop.
                        break;
                    else
                        continue;
                }
                //print each record.
                consumerRecords.forEach(record -> {
                    System.out.println("Record Key " + record.key());
                    System.out.println("Record value " + record.value());
                    System.out.println("Record partition " + record.partition());
                    System.out.println("Record offset " + record.offset());

                    MongoCollection<Document> coll = StoreService.getInstance().getTwitterDatabase().getCollection("Tweets");
                    Document emp1 = new Document();
                    emp1.put("key", record.value().getId());
                    emp1.put("value", record.value().toString());
                    coll.insertOne(emp1);
                });
                // commits the offset of record to broker.
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