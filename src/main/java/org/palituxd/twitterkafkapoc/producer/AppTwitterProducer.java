package org.palituxd.twitterkafkapoc.producer;

/**
 * Hello world!
 */
public class AppTwitterProducer {
    public static void main(String[] args) {
        System.out.println("Hello World! AppTwitterProducer");
        new TwitterKafkaProducer().run();
    }
}