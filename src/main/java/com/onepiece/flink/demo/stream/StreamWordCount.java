package com.onepiece.flink.demo.stream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 流处理统计字符
 *
 * @author fengyafei
 */
public class StreamWordCount {

  public static void main(String[] args) throws Exception {
    // 1、创建流处理执行环境
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

    // 2、读取文件
    DataStreamSource<String> dataSource = environment.readTextFile("input/word.txt");

    // 3、转换数据格式
    SingleOutputStreamOperator<Tuple2<String, Long>> returns = dataSource
        .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
          String[] words = line.split(" ");
          for (String word : words) {
            out.collect(Tuple2.of(word, 1L));
          }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

    // 4、分组
    KeyedStream<Tuple2<String, Long>, String> keyedStream = returns.keyBy(t -> t.f0);

    // 5、求和
    SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);

    // 打印
    sum.print();

    // 执行
    environment.execute();

  }


}
