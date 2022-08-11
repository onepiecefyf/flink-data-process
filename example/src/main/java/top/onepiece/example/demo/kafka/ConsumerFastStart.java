package top.onepiece.example.demo.kafka;

import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * @author fengyafei
 */
public class ConsumerFastStart {
  public static final String brokerList="124.223.219.45:9092";
  public static final String topic="test002";


  public static final String groupId="console-consumer-48702";

  public static void main(String args[]){

    //设置消费组的名称
    //将属性值反序列化
    Properties properties=new Properties();
    properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("bootstrap.servers",brokerList);
    properties.put("group.id",groupId);

    //创建一个消费者客户端实例
    org.apache.kafka.clients.consumer.KafkaConsumer<String,String> consumer=new org.apache.kafka.clients.consumer.KafkaConsumer(properties);

    //订阅主题
    consumer.subscribe(Collections.singletonList(topic));

    //循环消费消息
    while (true){
      ConsumerRecords<String,String> records=consumer.poll(1000L);
      for (ConsumerRecord<String,String> record:records){
        System.out.println("receiver a message from consumer client:"+record.value());
      }
    }
  }
}
