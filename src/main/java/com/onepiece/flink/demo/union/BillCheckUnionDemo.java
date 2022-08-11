package com.onepiece.flink.demo.union;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 使用合流CoProcessFunction 实现对账demo
 *
 * 声明了两个状态变量分别用来保存 App 的支付信息和第三方的支付信息。
 * App 的支付信息到达以后，会检查对应的第三方支付信息是否已经先到达（先到达会保存在对应的状态变量中），如果已经到达了，那么对账成功，直接输出对账成功的信息，
 * 并将保存第 三方支付消息的状态变量清空。
 *
 * 如果 App 对应的第三方支付信息没有到来，那么我们会注册 一个 5
 * 秒钟之后的定时器，也就是说等待第三方支付事件 5 秒钟。当定时器触发时，检查保存 app 支付信息的状态变量是否还在，
 * 如果还在，说明对应的第三方支付信息没有到来，所以输 出报警信息。
 *
 * @author fengyafei
 */
public class BillCheckUnionDemo {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setParallelism(1);

    // 来自APP的支付日志
    SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream =
        env.fromElements(Tuple3.of("Order-1", "APP", 1000L), Tuple3.of("Order-2", "APP", 2000L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                          @Override
                          public long extractTimestamp(
                              Tuple3<String, String, Long> element, long l) {
                            return element.f2;
                          }
                        }));

    // 来自其他平台支付
    SingleOutputStreamOperator<Tuple4<String, String, String, Long>> otherStream =
        env.fromElements(
                Tuple4.of("Order-1", "Other", "success", 1000L),
                Tuple4.of("Order-2", "Other", "success", 2000L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple4<String, String, String, Long>>forMonotonousTimestamps()
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                          @Override
                          public long extractTimestamp(
                              Tuple4<String, String, String, Long> element, long l) {
                            return element.f3;
                          }
                        }));

    // 检测同一支付订单中两条流是否匹配
    appStream
        .connect(otherStream)
        .keyBy(data -> data.f0, data -> data.f0)
        .process(new OrderMapResult())
        .print();

    env.execute();
  }

  public static class OrderMapResult
      extends CoProcessFunction<
          Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {

    private ValueState<Tuple3<String, String, Long>> appEventState;
    private ValueState<Tuple4<String, String, String, Long>> otherEventState;

    @Override
    public void open(Configuration parameters) throws Exception {
      appEventState =
          getRuntimeContext()
              .getState(
                  new ValueStateDescriptor<Tuple3<String, String, Long>>(
                      "APP-AGENT", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));

      otherEventState =
          getRuntimeContext()
              .getState(
                  new ValueStateDescriptor<Tuple4<String, String, String, Long>>(
                      "OTHER-AGENT",
                      Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)));
    }

    @Override
    public void close() throws Exception {}

    @Override
    public void processElement1(
        Tuple3<String, String, Long> app, Context context, Collector<String> collector)
        throws Exception {
      if (otherEventState.value() != null) {
        collector.collect("对账成功: " + app + " 当前其他支付平台状态： " + otherEventState.value());
        // 状态清空
        otherEventState.clear();
      } else {
        appEventState.update(app);
        //  注册一个 5 秒后的定时器，开始等待另一条流的事件
        context.timerService().registerEventTimeTimer(app.f2 + 5000L);
      }
    }

    @Override
    public void processElement2(
        Tuple4<String, String, String, Long> other, Context context, Collector<String> collector)
        throws Exception {
      if (appEventState.value() != null) {
        collector.collect("对账成功: " + other + " 当前APP支付状态： " + appEventState.value());
        // 状态清空
        appEventState.clear();
      } else {
        otherEventState.update(other);
        //  注册一个 5 秒后的定时器，开始等待另一条流的事件
        context.timerService().registerEventTimeTimer(other.f3 + 5000L);
      }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
        throws Exception {
      // 定时器触发，判断状态，如果某个状态不为空，说明另一条流中事件没来
      if (appEventState.value() != null) {
        out.collect("对账失败：" + appEventState.value() + " " + "第三方支付平台信息未到");
      }
      if (otherEventState.value() != null) {
        out.collect("对账失败：" + otherEventState.value() + " " + "app信息未到");
      }
      appEventState.clear();
      otherEventState.clear();
    }
  }
}
