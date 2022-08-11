package com.onepiece.flink.demo.batch;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理统计字符
 *
 * @author fengyafei
 */
public class BatchWordCount {

  public static void main(String[] args) throws Exception {
    // 1、创建环境
    ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

    // 2、从文件读取数据
    DataSource<String> dataSource = environment.readTextFile("input/word.txt");

    // 3、转换数据格式
    FlatMapOperator<String, Tuple2<String, Long>> flatMap =
        dataSource
            .flatMap(
                (String line, Collector<Tuple2<String, Long>> out) -> {
                  String[] words = line.split(" ");
                  for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                  }
                })
            .returns(Types.TUPLE(Types.STRING, Types.LONG));

    // 4、按照word进行分组
    UnsortedGrouping<Tuple2<String, Long>> wordGrouping = flatMap.groupBy(0);

    // 5、分组內聚合统计
    AggregateOperator<Tuple2<String, Long>> wordSum = wordGrouping.sum(1);

    // 6、打印结果
    wordSum.print();

  }
}
