package com.gzhu.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducer {
    public static void main(String[] args) {
        // 1.配置属性
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        // 指定对应的key和value序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        // 2.创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // 3.发送数据
        for (int i = 0; i < 300; i++) {
            producer.send(new ProducerRecord<>("topicx","data at 4.26" + i));
        }
        // 4.关闭资源
        producer.close();
    }
}
