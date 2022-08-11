package com.onepiece.flink.demo.process;

import com.onepiece.flink.demo.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * KeyProcessFunction 事件时间处理器
 *
 * @author fengyafei
 */
public class EventTimeTimerTest {

  public static void main(String[] args) throws Exception{
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    SingleOutputStreamOperator<Event> stream =
        env.addSource(new CustomSource())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<Event>() {
                          @Override
                          public long extractTimestamp(Event event, long recordTimestamp) {
                            return event.timestamp;
                          }
                        }));

    stream
        .keyBy(data -> true)
        .process(
            new KeyedProcessFunction<Boolean, Event, String>() {
              @Override
              public void processElement(Event event, Context context, Collector<String> collector)
                  throws Exception {

                collector.collect("数据到达，时间戳为：" + context.timestamp());
                collector.collect(
                    " 数据到达，水位线为：" + context.timerService().currentWatermark() + "\n -------分割线	");
                // 注册一个 10 秒后的定时器
                context.timerService().registerEventTimeTimer(context.timestamp() + 10 * 1000L);
              }

              @Override
              public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                  throws Exception {
                out.collect("定时器触发，触发时间：" + timestamp);
              }
            })
        .print();

    env.execute();
  }

  public static class CustomSource implements SourceFunction<Event> {
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
      // 直接发出测试数据
      ctx.collect(new Event("Mary", "./home", 1000L));
      // 为了更加明显，中间停顿 5 秒钟
      Thread.sleep(5000L);

      // 发出 10 秒后的数据
      ctx.collect(new Event("Mary", "./home", 11000L));
      Thread.sleep(5000L);

      // 发出 10 秒+1ms 后的数据
      ctx.collect(new Event("Alice", "./cart", 11001L));
      Thread.sleep(5000L);
    }

    @Override
    public void cancel() {}
  }
}
