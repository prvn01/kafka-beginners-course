package com.kpraveen.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        log.info("Simple Kafka Consumer");

        var bootstrapServer ="[::1]:9092";
        var topic = "demo_java";
        var group_id = "group1";


        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create a consumer

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(properties);

        //subscriber to a topic or list of topics

        consumer.subscribe(Arrays.asList(topic));

        //poll for the new data

        while(true){

            log.info("Polling");
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<Integer, String> record : records) {
                log.info("Key : {} , Value : {}",record.key(),record.value());
                log.info("Partition : {} , Offset :{} ",record.partition(),record.offset());
            }
        }




    }
}
