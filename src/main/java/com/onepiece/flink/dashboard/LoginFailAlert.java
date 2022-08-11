package com.onepiece.flink.dashboard;

import static com.onepiece.flink.common.CommonConst.APP_DSMP;
import static com.onepiece.flink.common.CommonConst.DASH_BOARD_OUT_TAG_RESPONSE;
import static com.onepiece.flink.common.CommonConst.DATA_FORMAT;
import static com.onepiece.flink.common.CommonConst.KAFKA_BOOTSTRAP_SERVERS;
import static com.onepiece.flink.common.CommonConst.KAFKA_GROUP_ID;
import static com.onepiece.flink.common.CommonConst.KAFKA_HTTP_DUMP_TOPIC;
import static com.onepiece.flink.common.CommonConst.KAFKA_PV_GROUP_ID;

import com.alibaba.fastjson.JSONObject;
import com.onepiece.flink.common.CommonConst;
import com.onepiece.flink.dashboard.entity.DsmpResponse;
import com.onepiece.flink.dashboard.function.DashboardAppMapFunction;
import com.onepiece.flink.dashboard.function.DashboardProcessFunction;
import com.onepiece.flink.dashboard.tag.DashboardOutputTag;
import com.onepiece.flink.dashboard.utils.DateUtils;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.OutputTag;

/**
 * 恶意登陆告警
 *
 * @author fengyafei
 */
public class LoginFailAlert {

  public static void main(String[] args) {
    // 1、创建执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // 2、创建kafka数据来源
    Properties properties = new Properties();
    properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, CommonConst.KAFKA_IP_PORT);
    properties.setProperty(KAFKA_GROUP_ID, KAFKA_PV_GROUP_ID);
    DataStream<String> streamFromKafka =
        env.addSource(
            new FlinkKafkaConsumer010<>(
                KAFKA_HTTP_DUMP_TOPIC, new SimpleStringSchema(), properties));

    // 3、二元组封装 平台->日志
    SingleOutputStreamOperator<Tuple2<String, String>> appMassageStream =
        streamFromKafka.map(new DashboardAppMapFunction());

    // 4、过滤平台、拆分请求和响应流
    OutputTag<String> responseTag = DashboardOutputTag.buildOutputTag(DASH_BOARD_OUT_TAG_RESPONSE);
    SingleOutputStreamOperator<String> requestStream =
        appMassageStream
            .filter(data -> data.f0.equals(APP_DSMP))
            .process(new DashboardProcessFunction(responseTag));

    // 5、处理响应流
    DataStream<String> sideOutput = requestStream.getSideOutput(responseTag);
    DataStream<DsmpResponse> dataStream =
        sideOutput
            .map(
                data -> {
                  return JSONObject.parseObject(data, DsmpResponse.class);
                })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<DsmpResponse>forMonotonousTimestamps()
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<DsmpResponse>() {
                          @Override
                          public long extractTimestamp(DsmpResponse response, long l) {
                            Long timestamp =
                                DateUtils.transformStringToLong(
                                    response.getTimestamp(), DATA_FORMAT);
                            return timestamp;
                          }
                        }));
  }
}
