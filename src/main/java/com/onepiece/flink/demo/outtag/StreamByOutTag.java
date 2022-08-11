package com.onepiece.flink.demo.outtag;

import com.onepiece.flink.demo.entity.Event;
import com.onepiece.flink.demo.source.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Tuple3;

/**
 * 侧输出流demo
 * @author fengyafei
 */
public class StreamByOutTag {

  private static OutputTag<Tuple3<String, String, Long>> MaryTag = new
      OutputTag<Tuple3<String, String, Long>>("Mary-pv"){};


  private static OutputTag<Tuple3<String, String, Long>> BobTag = new OutputTag<Tuple3<String, String, Long>>("Bob-pv"){};

  public static void main(String[] args) throws Exception{
    StreamExecutionEnvironment env = StreamExecutionEnvironment
        .getExecutionEnvironment();

    env.setParallelism(1);

    DataStreamSource<Event> streamSource = env.addSource(new ClickSource());

    SingleOutputStreamOperator<Event> process = streamSource
        .process(new ProcessFunction<Event, Event>() {
          @Override
          public void processElement(Event event, Context context, Collector<Event> collector)
              throws Exception {
            if (event.user.equals("Bob")) {
              context.output(BobTag, new Tuple3<>(event.user, event.url, event.timestamp));
            } else if (event.user.equals("Mary")) {
              context.output(MaryTag, new Tuple3<>(event.user, event.url, event.timestamp));
            } else {
              collector.collect(event);
            }
          }
        });

    DataStream<Tuple3<String, String, Long>> sideOutput = process.getSideOutput(MaryTag);
    DataStream<Tuple3<String, String, Long>> sideOutput1 = process.getSideOutput(BobTag);

    sideOutput.print("Mary");
    sideOutput1.print("Bob");
    process.print("else");

    env.execute();


  }
}
