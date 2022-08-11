package com.onepiece.flink.demo.httpdump;

import com.onepiece.flink.demo.kafka.MyNoParalleSource;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

/**
 * @author fengyafei
 */
public class HttpDumpLogProducer {

  public static final String BROKER="124.223.219.45:9092";
  public static final String TOPIC="test002";
  public static final String GROUP_ID="test";

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    Properties property = new Properties();
    property.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    property.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    property.setProperty("bootstrap.servers", BROKER);
    property.put("group.id",GROUP_ID);
    FlinkKafkaProducer010 producer010 =
        new FlinkKafkaProducer010<>(TOPIC, new SimpleStringSchema(), property);

    DataStream<String> stream = environment.addSource(new MyNoParalleSource());
    stream.addSink(producer010);

    environment.execute();
  }
}
