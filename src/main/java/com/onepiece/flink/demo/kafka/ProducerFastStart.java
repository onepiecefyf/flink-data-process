package com.onepiece.flink.demo.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerFastStart {
  public static final String brokerList="124.223.219.45:9092";
  public static final String topic="test002";

  public static void main(String[] args){

    //配置生产者客户端参数
    //将配置序列化
    Properties properties=new Properties();
    properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
    properties.put("bootstrap.servers",brokerList);

    //创建KafkaProducer 实例
    org.apache.kafka.clients.producer.KafkaProducer<String,String> producer=new org.apache.kafka.clients.producer.KafkaProducer(properties);

    //构建待发送的消息
    ProducerRecord<String,String> record=new ProducerRecord(topic,"hello Kafka!");
    try {
      //尝试发送消息
      producer.send(record);
      //打印发送成功
      System.out.println("send success from producer");
    } catch (Exception e) {
      e.printStackTrace();
    }finally {
      //关闭生产者客户端实例
      producer.close();
      System.out.println("关闭成功");
    }
  }
}
