package com.onepiece.flink.demo.process;

import com.onepiece.flink.demo.entity.Event;
import com.onepiece.flink.demo.source.ClickSource;
import java.sql.Timestamp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * KeyProcessFunction 时间处理定时器
 *
 * 间隔10s之后执行第一次定时器触发，因为设置的定时器时间为10s后
 *
 * @author fengyafei
 */
public class ProcessingTimeTimerTest {

  public static void main(String[] args) throws Exception{
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStreamSource<Event> stream = env.addSource(new ClickSource());

    stream
        .keyBy(data -> true)
        .process(
            new KeyedProcessFunction<Boolean, Event, String>() {
              @Override
              public void processElement(Event o, Context context, Collector<String> collector)
                  throws Exception {

                long current = context.timerService().currentProcessingTime();
                collector.collect("数据到达，到达时间：" + new Timestamp(current));

                // 注册一个10秒后的定时器
                context.timerService().registerProcessingTimeTimer(current + 10 * 1000L);
              }

              @Override
              public void onTimer(
                  long timestamp, OnTimerContext context, Collector<String> collector)
                  throws Exception {
                collector.collect("定时器触发时间：" + new Timestamp(timestamp));
              }
            })
        .print();

    env.execute();
  }
}
