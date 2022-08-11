package top.onepiece.dashboard;

import static com.onepiece.flink.common.CommonConst.APP_DSMP;
import static com.onepiece.flink.common.CommonConst.DATA_FORMAT;
import static com.onepiece.flink.common.CommonConst.DSMP_PV_JOB;
import static com.onepiece.flink.common.CommonConst.KAFKA_BOOTSTRAP_SERVERS;
import static com.onepiece.flink.common.CommonConst.KAFKA_GROUP_ID;
import static com.onepiece.flink.common.CommonConst.KAFKA_HTTP_DUMP_TOPIC;
import static com.onepiece.flink.common.CommonConst.KAFKA_PV_GROUP_ID;

import com.alibaba.fastjson.JSONObject;
import com.onepiece.flink.common.CommonConst;
import com.onepiece.flink.dashboard.agg.PvCountAgg;
import com.onepiece.flink.dashboard.entity.DsmpRequest;
import com.onepiece.flink.dashboard.entity.UrlCountView;
import com.onepiece.flink.dashboard.function.DashboardAppMapFunction;
import com.onepiece.flink.dashboard.function.DashboardProcessFunction;
import com.onepiece.flink.dashboard.function.PvResultCountFunction;
import com.onepiece.flink.dashboard.function.TotalPvCountFunction;
import com.onepiece.flink.dashboard.utils.DateUtils;
import com.onepiece.flink.dashboard.utils.StrUtils;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

/**
 * url访问总量统计
 *
 * @author fengyafei
 */
public class DashboardUrlCountTotalMain {

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
    SingleOutputStreamOperator<String> requestStream =
        appMassageStream
            .filter(data -> data.f0.equals(APP_DSMP))
            .process(new DashboardProcessFunction());

    // 5、处理请求流
    DataStream<DsmpRequest> dataStream =
        requestStream
            .map(
                data -> {
                  DsmpRequest request = JSONObject.parseObject(data, DsmpRequest.class);
                  request.setRequestUri(StrUtils.subRequestURL(request.getRequestUri()));
                  return request;
                })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<DsmpRequest>forMonotonousTimestamps()
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

    // 6、开窗、过滤非必要url、以url分组
    SingleOutputStreamOperator<UrlCountView> sumStream =
        dataStream
            .filter(
                data -> {
                  String regex = "^((?!\\.(css|js|png|ico|woff)$).)*$";
                  return Pattern.matches(regex, data.getRequestUri());
                })
            .map(
                new MapFunction<DsmpRequest, Tuple2<String, Long>>() {
                  @Override
                  public Tuple2<String, Long> map(DsmpRequest request) throws Exception {
                    // 存在多个分区前提下，解决分区执行下，数据倾斜问题
                    // Random random = new Random()
                    return new Tuple2<>(request.getRequestUri(), 1L);
                  }
                })
            .keyBy(data -> data.f0)
            .timeWindow(Time.seconds(10))
            .aggregate(new PvCountAgg(), new PvResultCountFunction());

    // 7、将各区数据汇总
    SingleOutputStreamOperator<UrlCountView> totalStream =
        sumStream.keyBy(UrlCountView::getWindowEnd).process(new TotalPvCountFunction());

    requestStream.print("request");
    totalStream.print("total");

    env.execute(DSMP_PV_JOB);
  }
}
