package com.nuc.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {

    public static void main(String[] args) {

        // 1、创建消费者配置信息
        Properties properties = new Properties();

        // 2、给配置信息赋值

        // 连接的集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092");
        // 开启自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 自动提交的延时
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");


        // Key，Value的反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // 消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdata");

        // 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 订阅主题
        consumer.subscribe(Arrays.asList("first", "third"));

        // 获取数据
        while(true){
            ConsumerRecords<String, String> comsumerRecords = consumer.poll(100);

            // 解析并打印comsumerRecords
            for (ConsumerRecord<String, String> comsumerRecord : comsumerRecords) {

                System.out.println(comsumerRecord.key() + "--" + comsumerRecord.value());

            }
        }



    }

}
