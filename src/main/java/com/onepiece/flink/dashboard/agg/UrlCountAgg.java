package com.onepiece.flink.dashboard.agg;

import com.onepiece.flink.dashboard.entity.DsmpRequest;
import org.apache.flink.api.common.functions.AggregateFunction;
/**
 * 统计Url TopN
 * @author fengyafei
 */
public class UrlCountAgg implements AggregateFunction<DsmpRequest, Long, Long> {

  @Override
  public Long createAccumulator() {
    return 0L;
  }

  @Override
  public Long add(DsmpRequest request, Long count) {
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
