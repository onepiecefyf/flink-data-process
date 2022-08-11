package com.onepiece.flink.dashboard;

import static com.onepiece.flink.common.CommonConst.APP_DSMP;
import static com.onepiece.flink.common.CommonConst.DATA_FORMAT;
import static com.onepiece.flink.common.CommonConst.DSMP_URL_TOP;
import static com.onepiece.flink.common.CommonConst.KAFKA_BOOTSTRAP_SERVERS;
import static com.onepiece.flink.common.CommonConst.KAFKA_GROUP_ID;
import static com.onepiece.flink.common.CommonConst.KAFKA_HTTP_DUMP_TOPIC;
import static com.onepiece.flink.common.CommonConst.KAFKA_TOP_GROUP_ID;
import static com.onepiece.flink.common.CommonConst.LATE_TAG;
import static com.onepiece.flink.common.CommonConst.LOG_REQUEST_RESPONSE_SPLIT;

import com.alibaba.fastjson.JSONObject;
import com.onepiece.flink.common.CommonConst;
import com.onepiece.flink.dashboard.agg.TopNHotUrls;
import com.onepiece.flink.dashboard.agg.UrlCountAgg;
import com.onepiece.flink.dashboard.agg.UrlCountResult;
import com.onepiece.flink.dashboard.entity.DsmpRequest;
import com.onepiece.flink.dashboard.entity.UrlCountView;
import com.onepiece.flink.dashboard.function.DashboardAppMapFunction;
import com.onepiece.flink.dashboard.function.DashboardProcessFunction;
import com.onepiece.flink.dashboard.tag.DashboardOutputTag;
import com.onepiece.flink.dashboard.utils.DateUtils;
import com.onepiece.flink.dashboard.utils.StrUtils;
import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.OutputTag;

/**
 * 数据安全管控平台大屏展示
 *
 * @author fengyafei
 */
public class DashboardScreenMain {

  public static void main(String[] args) throws Exception {
    // 1、创建执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // 2、创建kafka数据来源
    Properties properties = new Properties();
    properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, CommonConst.KAFKA_IP_PORT);
    properties.setProperty(KAFKA_GROUP_ID, KAFKA_TOP_GROUP_ID);
    DataStream<String> streamFromKafka =
        env.addSource(
            new FlinkKafkaConsumer010<>(
                KAFKA_HTTP_DUMP_TOPIC, new SimpleStringSchema(), properties));

    // 3、二元组封装 平台->日志
    SingleOutputStreamOperator<Tuple2<String, String>> appMassageStream =
        streamFromKafka.map(new DashboardAppMapFunction());

    // 侧输出流 分割请求和响应流
    OutputTag<String> outputTag = DashboardOutputTag.buildOutputTag(LOG_REQUEST_RESPONSE_SPLIT);

    // 4、过滤平台、拆分请求和响应流
    SingleOutputStreamOperator<String> requestStream =
        appMassageStream
            .filter(data -> data.f0.equals(APP_DSMP))
            .process(new DashboardProcessFunction(outputTag));

    // 5、处理请求流
    DataStream<DsmpRequest> requestWaterMarkerStream =
        requestStream
            .map(
                data -> {
                  DsmpRequest request = JSONObject.parseObject(data, DsmpRequest.class);
                  request.setRequestUri(StrUtils.subRequestURL(request.getRequestUri()));
                  return request;
                })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<DsmpRequest>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<DsmpRequest>() {
                          @Override
                          public long extractTimestamp(DsmpRequest request, long l) {
                            Long timestamp =
                                DateUtils.transformStringToLong(
                                    request.getTimestamp(), DATA_FORMAT);
                            return timestamp;
                          }
                        }));

    OutputTag<DsmpRequest> lateTag = DashboardOutputTag.buildlateTag(LATE_TAG);

    // 6、开窗、过滤非必要url、以url分组
    SingleOutputStreamOperator<UrlCountView> aggregateStream =
        requestWaterMarkerStream
            .filter(
                data -> {
                  String regex = "^((?!\\.(css|js|png|ico|woff)$).)*$";
                  return Pattern.matches(regex, data.getRequestUri());
                })
            .keyBy(DsmpRequest::getRequestUri)
            .timeWindow(Time.minutes(10), Time.seconds(5))
            .allowedLateness(Time.minutes(1))
            .sideOutputLateData(lateTag)
            .aggregate(new UrlCountAgg(), new UrlCountResult());

    // 收集同一窗口count数据，排序输出
    DataStream<String> resultStream =
        aggregateStream.keyBy(UrlCountView::getWindowEnd).process(new TopNHotUrls(5));

    DataStream<DsmpRequest> sideOutput = aggregateStream.getSideOutput(lateTag);

    sideOutput.print("late");
    requestStream.print("request");
    resultStream.print();

    env.execute(DSMP_URL_TOP);
  }
}
