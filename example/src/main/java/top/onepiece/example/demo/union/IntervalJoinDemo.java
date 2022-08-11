package top.onepiece.example.demo.union;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import top.onepiece.example.demo.entity.Event;

/**
 * 间隔联结demo
 *
 * 在间隔时间内，对于未找到匹配的数据，直接丢弃
 *
 * @author fengyafei
 */
public class IntervalJoinDemo {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    SingleOutputStreamOperator<Tuple3<String, String, Long>> orderStream =
        env.fromElements(
                Tuple3.of("Mary", "order-1", 5000L),
                Tuple3.of("Alice", "order-2", 5000L),
                Tuple3.of("Bob", "order-3", 20000L),
                Tuple3.of("Alice", "order-4", 20000L),
                Tuple3.of("Cary", "order-5", 51000L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                          @Override
                          public long extractTimestamp(
                              Tuple3<String, String, Long> element, long recordTimestamp) {
                            return element.f2;
                          }
                        }));

    SingleOutputStreamOperator<Event> clickStream =
        env.fromElements(
                new Event("Mary", "./cart", 5000L),
                new Event("Alice", "./prod?id=1", 5000L),
                new Event("Bob", "./prod?id=2", 20000L),
                new Event("Alice", "./prod?id=2", 20000L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<Event>() {
                          @Override
                          public long extractTimestamp(Event element, long recordTimestamp) {
                            return element.timestamp;
                          }
                        }));

    orderStream
        .keyBy(data -> data.f0)
        .intervalJoin(clickStream.keyBy(data -> data.user))
        .between(Time.seconds(-5), Time.seconds(10))
        .process(
            new ProcessJoinFunction<Tuple3<String, String, Long>, Event, String>() {
              @Override
              public void processElement(
                  Tuple3<String, String, Long> left,
                  Event right,
                  Context context,
                  Collector<String> collector)
                  throws Exception {
                collector.collect(left + " => " + right);
              }
            })
        .print();

    env.execute();
  }
}
