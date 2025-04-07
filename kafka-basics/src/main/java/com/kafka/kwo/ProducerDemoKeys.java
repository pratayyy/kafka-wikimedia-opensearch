package com.kafka.kwo;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) throws InterruptedException {

        log.info("I am a Kafka Producer!");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "400"); // increasing batch size to achieve record partition spread
//        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName()); // default is StickyPartitioning which promotes batching

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {
                // create a producer record
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello world " + i;
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                // send data - asynchronous
                producer.send(producerRecord, (recordMetadata, e) -> {
                    // executes every time a record is successfully send or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        log.info("Key: {} | Partitions: {}", key, recordMetadata.partition());
                    } else {
                        log.error("Error while producing", e);
                    }
                });
            }

            Thread.sleep(1000);
        }

        // flush data - synchronous
        producer.flush(); // tell the producer to send all data and block until done

        // flush and close producer
        producer.close();
    }
}
