package com.gzhu.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerParameters {
    public static void main(String[] args) {
        // 1.配置属性
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.10.102:9092");
        // 指定对应的key和value序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // 更改缓冲区大小 默认33554432 = 32M
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        // 批次大小 默认16K 16384
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        // linger.ms  1ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);
        // 压缩类型 snappy用多一些
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,1000);



        // 2.创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // 3.发送数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first","Hello " + i));
        }

        // 4.关闭资源
        producer.close();

    }
}
