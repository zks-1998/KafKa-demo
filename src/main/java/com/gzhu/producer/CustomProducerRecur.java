package com.gzhu.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerRecur {
    public static void main(String[] args) {
        // 1.配置属性
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        // 指定对应的key和value序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.gzhu.producer.MyPartitioner");


        // 2.创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // 3.发送数据
        producer.send(new ProducerRecord<>("topicx","Producer to topicx -- callback"), new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e == null){
                    System.out.println("发送到的主题：" + recordMetadata.topic());
                    System.out.println("发送到的分区："+recordMetadata.partition());
                }
            }
        });

        // 4.关闭资源
        producer.close();

    }
}
