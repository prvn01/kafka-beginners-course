package com.kpraveen.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerWithKeys.class);

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"[::1]:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<Integer,String > producer = new KafkaProducer(properties);

        for(int i=0;i<10;i++) {
            var topic = "demo_java";
            Integer key =i;
            String value = "Hello "+i;

            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topic,key,value);

            producer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    log.info("Recieved with metadata / \n" +
                            "Topic : {} \n Key : {} \n Partition : {} \n Offset : {} \n Timestamp : {}", metadata.topic(), producerRecord.key(),metadata.partition(), metadata.offset(), metadata.timestamp()
                    );
                } else {
                    log.error("Error while producing", exception);
                }
            });


        }
        producer.flush();
        producer.close();
    }
}
