package top.onepiece.example.demo.union;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 窗口联结demo
 *
 * @author fengyafei
 */
public class WindowJoinDemo {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<Tuple2<String, Long>> firstStream =
        env.fromElements(
                Tuple2.of("a", 1000L),
                Tuple2.of("b", 1000L),
                Tuple2.of("a", 2000L),
                Tuple2.of("b", 2000L))
            .assignTimestampsAndWatermarks(
                // 当前一段时间最大时间戳作为水位线
                WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                          @Override
                          public long extractTimestamp(Tuple2<String, Long> element, long l) {
                            return element.f1;
                          }
                        }));

    DataStream<Tuple2<String, Long>> secondStream =
        env.fromElements(
                Tuple2.of("a", 3000L),
                Tuple2.of("b", 3000L),
                Tuple2.of("a", 4000L),
                Tuple2.of("b", 4000L))
            .assignTimestampsAndWatermarks(
                // 当前一段时间最大时间戳作为水位线
                WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                          @Override
                          public long extractTimestamp(Tuple2<String, Long> element, long l) {
                            return element.f1;
                          }
                        }));

    DataStream<String> result = firstStream
        .join(secondStream)
        // 以第一个参数（用户）作为联结key
        .where(data -> data.f0)
        .equalTo(data -> data.f0)
        // 使用滚动窗口，设置延迟时间是5s
        .window(TumblingEventTimeWindows.of(Time.seconds(100)))
        .apply(
            new JoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
              @Override
              public String join(Tuple2<String, Long> left, Tuple2<String, Long> right)
                  throws Exception {
                return left + " => " + right;
              }
            });

    result.print();

    env.execute();
  }
}
