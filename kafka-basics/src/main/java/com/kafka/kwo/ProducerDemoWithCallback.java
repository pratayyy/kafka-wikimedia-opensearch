package com.kafka.kwo;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) throws InterruptedException {

        log.info("I am a Kafka Producer!");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "400"); // Increasing batch size to achieve record partition spread
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName()); // Default is StickyPartitioning which promotes batching

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 30; i++) {
                // create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world" + i);

                // send data - asynchronous
                producer.send(producerRecord, (recordMetadata, e) -> {
                    // executes every time a record is successfully send or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        log.info("Received new metadata \n Topic: {} \n Partition: {} \n Offset: {} \n Timestamp: {}", recordMetadata.topic(),
                                recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                    } else {
                        log.error("Error while producing", e);
                    }
                });
            }

            Thread.sleep(1000); // to simulate records going to different partitions
        }

        // flush data - synchronous
        producer.flush(); // tell the producer to send all data and block until done

        // flush and close producer
        producer.close();
    }
}
