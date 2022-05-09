package com.gzhu.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TransactionsTest {
    public static void main(String[] args) {
        // 1.配置属性
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.10.102:9092");
        // 指定对应的key和value序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // 指定事务id  任意取，但唯一
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"100");


        // 2.创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        // 3.初始化并开启事务
        producer.initTransactions();
        producer.beginTransaction();

        try {
            // 4.发送数据
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>("first","Hello " + i));
            }
            // 5.提交事务
            producer.commitTransaction();
        }catch (Exception e){
            // 有异常终止事务
            producer.abortTransaction();
        }finally {
            // 6.关闭生产者
            producer.close();
        }
    }
}
