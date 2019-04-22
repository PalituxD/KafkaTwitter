package org.palituxd.kafkapoc.producer;

/**
 * Hello world!
 */
public class AppProducer {
    public static void main(String[] args) {
        System.out.println("Hello World! AppProducer");
        new CustomKafkaProducer().run();
    }
}
