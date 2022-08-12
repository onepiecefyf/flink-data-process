package top.onepiece.login;

import java.net.URL;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import top.onepiece.login.function.LoginFailPatternAlertFunction;
import top.onepiece.model.login.LoginEvent;
import top.onepiece.model.login.LoginFailAlertMsg;

/**
 * @author fengyafei
 */
public class LoginFailWithCep {
  public static void main(String[] args) throws Exception {

    // 获取执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // 获取数据文件
    URL resource = LoginFailWithCep.class.getResource("/LoginLog.csv");
    DataStreamSource<String> streamSource = env.readTextFile(resource.getPath());

    // 开启水位线 处理无序流 延迟3s
    SingleOutputStreamOperator<LoginEvent> loginStream =
        streamSource
            .map(
                line -> {
                  String[] split = line.split(",");
                  return new LoginEvent(
                      Long.valueOf(split[0]), split[1], split[2], Long.valueOf(split[3]));
                })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<LoginEvent>() {
                          @Override
                          public long extractTimestamp(LoginEvent loginEvent, long l) {
                            return loginEvent.getTimestamp() * 1000L;
                          }
                        }));

    // 定义规则 5s內登陆失败三次 告警
    Pattern<LoginEvent, LoginEvent> loginEvents =
        Pattern.<LoginEvent>begin("failEvents")
            .where(
                new SimpleCondition<LoginEvent>() {
                  @Override
                  public boolean filter(LoginEvent loginEvent) throws Exception {
                    return "fail".equals(loginEvent.getStatus());
                  }
                })
            .times(3)
            .consecutive()
            .within(Time.seconds(5));

    // 将匹配模式应用到数据流
    PatternStream<LoginEvent> patternStream =
        CEP.pattern(loginStream.keyBy(LoginEvent::getUserId), loginEvents);

    // 检出符合匹配条件的复杂事件，进行转换处理，得到报警信息
    SingleOutputStreamOperator<LoginFailAlertMsg> select =
        patternStream.select(new LoginFailPatternAlertFunction());

    select.print();
    env.execute("login-cep-jop");
  }
}
