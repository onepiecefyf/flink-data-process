package com.onepiece.kafka.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Topic 生产者
 *
 * @author fengyafei
 */
public class ProducerTopic {

  private static final String TOPIC = "test";
  private static final String BOOTSTRAP_SERVER = "120.48.81.162:9092";

  public static void main(String[] args) {

    // 创建配置文件
    Properties properties = new Properties();
    // 添加kafka连接信息
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    // 创建kafka生产者
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    producer.send(
        new ProducerRecord<String, String>(TOPIC, "hello"),
        new Callback() {
          @Override
          public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (null == e) {
              System.out.println(
                  " 主题： " + recordMetadata.topic() + "->" + "分区：" + recordMetadata.partition());
            } else {
              e.printStackTrace();
            }
          }
        });

    producer.close();
  }
}
