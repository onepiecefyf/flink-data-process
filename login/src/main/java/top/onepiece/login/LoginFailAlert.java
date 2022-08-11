package top.onepiece.login;

import static top.onepiece.common.common.CommonConst.APP_DSMP;
import static top.onepiece.common.common.CommonConst.DASH_BOARD_OUT_TAG_RESPONSE;
import static top.onepiece.common.common.CommonConst.DATA_FORMAT;
import static top.onepiece.common.common.CommonConst.KAFKA_BOOTSTRAP_SERVERS;
import static top.onepiece.common.common.CommonConst.KAFKA_GROUP_ID;
import static top.onepiece.common.common.CommonConst.KAFKA_HTTP_DUMP_TOPIC;
import static top.onepiece.common.common.CommonConst.KAFKA_PV_GROUP_ID;

import com.alibaba.fastjson.JSONObject;
import java.time.Duration;
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
import top.onepiece.common.common.CommonConst;
import top.onepiece.common.function.DashboardAppMapFunction;
import top.onepiece.common.function.DashboardProcessFunction;
import top.onepiece.common.tag.DashboardOutputTag;
import top.onepiece.common.utils.DateUtils;
import top.onepiece.login.function.LoginFailAlertFunction;
import top.onepiece.model.base.DsmpResponse;

/**
 * 恶意登陆告警任务 1、三秒内、连续登陆失败两次属于恶意登陆
 *
 * <p>处理流程： 1、定义水位线延迟时间为3s 2、保存用户登陆失败状态 超过两次告警
 *
 * @author fengyafei
 */
public class LoginFailAlert {

  public static void main(String[] args) throws Exception {
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

    // 5、处理响应流 设置延迟水位线 3s
    DataStream<String> sideOutput = requestStream.getSideOutput(responseTag);
    DataStream<DsmpResponse> responseStream =
        sideOutput
            .map(
                data -> {
                  return JSONObject.parseObject(data, DsmpResponse.class);
                })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<DsmpResponse>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<DsmpResponse>() {
                          @Override
                          public long extractTimestamp(DsmpResponse response, long l) {
                            return DateUtils.transformStringToLong(
                                response.getTimestamp(), DATA_FORMAT);
                          }
                        }));

    // 6、 根据用户ID分组 聚合计算
    responseStream.keyBy(DsmpResponse::getSeq).process(new LoginFailAlertFunction(2));

    responseStream.print();

    env.execute("login-fail-job");
  }
}
