package org.palituxd.kafkapoc.consumer;

/**
 * Hello world!
 */
public class AppConsumer {
    public static void main(String[] args) {
        System.out.println("Hello World! AppConsumer");
        new CustomKafkaConsumer().run();
    }
}
