package com.gzhu.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class CustomConsumerPartition {
    public static void main(String[] args) {
        // 1.配置
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        // 2.反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);

        // 3.配置消费者id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"kun103");


        // 4.创建消费者
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        // 5.订阅主题
        ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(new TopicPartition("topicx",0));
        consumer.assign(topicPartitions);

        // 6.消费
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.topic() +" " + record.partition() + " "+ record.value());
            }
        }

    }
}
