package top.onepiece.example.demo.httpdump;

import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

/** @author fengyafei */
public class HttpDumpLogConsumer {

  public static final String BROKER = "124.223.219.45:9092";
  public static final String TOPIC = "test002";
  public static final String GROUP_ID = "test";

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    // 开启checkpoint 时间间隔为毫秒
    environment.enableCheckpointing(5000L);

    Properties property = new Properties();
    // kafka broker地址
    property.setProperty("bootstrap.servers", BROKER);
    // 自动偏移量提交
    property.put("enable.auto.commit", true);
    // 偏移量提交的时间间隔，毫秒
    property.put("auto.commit.interval.ms", 5000);
    // 消费组
    property.setProperty("group.id", GROUP_ID);
    // kafka 消息key序列化器
    property.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    // kafka 消息value序列化器
    property.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    FlinkKafkaConsumer010 consumer =
        new FlinkKafkaConsumer010<>(TOPIC, new SimpleStringSchema(), property);
    // 设置checkpoint后在提交offset，即onCheckpoint模式
    // 该值默认为true，
    consumer.setCommitOffsetsOnCheckpoints(true);

    DataStream<String> stream = environment.addSource(consumer);
    stream.print();

    environment.execute();
  }
}
