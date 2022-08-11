package com.onepiece.flink.dashboard.agg;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * PV总数统计
 *
 * @author fengyafei
 */
public class PvCountAgg implements AggregateFunction<Tuple2<String, Long>, Long, Long> {

  @Override
  public Long createAccumulator() {
    return 0L;
  }

  @Override
  public Long add(Tuple2<String, Long> value, Long count) {
    return count + 1;
  }

  @Override
  public Long getResult(Long count) {
    return count;
  }

  @Override
  public Long merge(Long a, Long b) {
    return a + b;
  }
}
