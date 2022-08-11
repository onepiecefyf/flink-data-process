package com.onepiece.flink.demo.process;

import com.onepiece.flink.demo.entity.Event;
import com.onepiece.flink.demo.source.ClickSource;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 使用ProcessFunction实现TopN案例分析
 *
 * @author fengyafei
 */
public class ProcessAllWindowTopN {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // 创建时间戳水位线
    SingleOutputStreamOperator<Event> eventStream =
        env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<Event>() {
                          @Override
                          public long extractTimestamp(Event event, long l) {
                            return event.timestamp;
                          }
                        }));

    // 以url为key
    SingleOutputStreamOperator<String> result =
        eventStream.map(
            new MapFunction<Event, String>() {
              @Override
              public String map(Event event) throws Exception {
                return event.url;
              }
              // 开启滑动窗口 统计最近10s 每5s中更新一次
            })
        .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
        .process(
            new ProcessAllWindowFunction<String, String, TimeWindow>() {
              @Override
              public void process(
                  Context context, Iterable<String> elements, Collector<String> collector)
                  throws Exception {

                // 统计url访问结果
                HashMap<String, Long> urlCountMap = new HashMap<>();
                for (String element : elements) {
                  if (urlCountMap.containsKey(element)) {
                    Long count = urlCountMap.get(element);
                    urlCountMap.put(element, count + 1);
                  } else {
                    urlCountMap.put(element, 1L);
                  }
                }

                // 将map转为list
                ArrayList<Tuple2<String, Long>> tuple2s = new ArrayList<>();
                for (String key : urlCountMap.keySet()) {
                  tuple2s.add(Tuple2.of(key, urlCountMap.get(key)));
                }

                // 排序
                tuple2s.sort(
                    new Comparator<Tuple2<String, Long>>() {
                      @Override
                      public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                        return o2.f1.intValue() - o1.f1.intValue();
                      }
                    });

                // 取排序后的前两名，构建输出结果
                StringBuilder result = new StringBuilder();

                result.append("========================================\n");
                for (int i = 0; i < 2; i++) {
                  Tuple2<String, Long> temp = tuple2s.get(i);
                  String info =
                      "浏览量 No."
                          + (i + 1)
                          + " url："
                          + temp.f0
                          + " 浏览量："
                          + temp.f1
                          + "	窗	口	结	束	时	间	：	"
                          + new Timestamp(context.window().getEnd())
                          + "\n";

                  result.append(info);
                }
                result.append("========================================\n");

                collector.collect(result.toString());
              }
            });


    eventStream.print();
    result.print();

    env.execute();
  }
}
