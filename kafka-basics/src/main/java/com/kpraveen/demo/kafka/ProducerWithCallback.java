package com.kpraveen.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallback.class);

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"[::1]:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String > producer = new KafkaProducer(properties);

        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java","Hello from async producer");

        producer.send(producerRecord,(metadata, exception) -> {
            if(exception == null) {
                log.info("Recieved with metadata / \n" +
                        "Topic : {} \n Partition : {} \n Offset : {} \n Timestamp : {}",metadata.topic(),metadata.partition(),metadata.offset(),metadata.timestamp()
                        );
            } else {
                log.error("Error while producing",exception);
            }
        });

        producer.flush();
        producer.close();


    }
}
