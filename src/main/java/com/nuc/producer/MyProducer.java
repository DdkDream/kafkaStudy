package com.nuc.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.ClientInfoStatus;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyProducer {
    public static void main(String[] args) throws InterruptedException, ExecutionException {

        // 1、创建Kafka生产者的配置信息
        Properties properties = new Properties();

        // 2、指定连接的Kafka集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop101:9092");

        // 3、ACK应答级别
        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        // 4、重试次数
        properties.put("retries", 1);

        // 5、批次大小
        properties.put("batch.size", 16384);

        // 6、等待时间
        properties.put("linger.ms", 1);

        // 7、RecordAccumulator 缓冲区大小
        properties.put("buffer.memory", 33554432);

        // 8、Key，Value的序列化类
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        // 9、创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 10、发送数据
        for(int i = 0; i < 10; i++){
            producer.send(new ProducerRecord<>("first", "nuc", "atguigu--" + i)).get();
        }

//        Thread.sleep(100);

        // 11、关闭资源
        producer.close();



    }
}
