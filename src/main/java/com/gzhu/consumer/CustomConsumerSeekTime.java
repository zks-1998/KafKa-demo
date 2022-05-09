package com.gzhu.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class CustomConsumerSeekTime {
    public static void main(String[] args) {
        // 1.配置
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        // 2.反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);

        // 3.配置消费者id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"1111");

        // 4.创建消费者
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        // 5.订阅主题
        ArrayList<String> topics = new ArrayList<>();
        topics.add("topicx");
        consumer.subscribe(topics);

        // 6.指定offset
        // 6.1 获取当前主题的分区信息
        Set<TopicPartition> assignment = consumer.assignment();
        // 6.2 保证分区已存在
        while(assignment.size() == 0){
            consumer.poll(Duration.ofSeconds(1));
            assignment = consumer.assignment();
        }
        // 6.3 指定offset的时间 每个分区对应时间
        HashMap<TopicPartition, Long> hashMap = new HashMap<>();
        for(TopicPartition topicPartition : assignment){
           hashMap.put(topicPartition,System.currentTimeMillis() - 24 * 3600 * 1000);
        }
        // 通过时间获取offset
        Map<TopicPartition, OffsetAndTimestamp> map = consumer.offsetsForTimes(hashMap);
        // 6.4 指定offset
        for(TopicPartition topicPartition : assignment){
            OffsetAndTimestamp time = map.get(topicPartition);

            consumer.seek(topicPartition,time.offset());
        }


        // 7.消费
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }

    }
}
