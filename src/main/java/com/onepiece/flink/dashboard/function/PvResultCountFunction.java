package com.onepiece.flink.dashboard.function;

import com.onepiece.flink.dashboard.entity.UrlCountView;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 窗口聚合结果
 *
 * @author fengyafei
 */
public class PvResultCountFunction
    implements WindowFunction<Long, UrlCountView, String, TimeWindow> {

  @Override
  public void apply(
      String partition, TimeWindow window, Iterable<Long> input, Collector<UrlCountView> out)
      throws Exception {
    out.collect(new UrlCountView(partition, window.getEnd(), input.iterator().next()));
  }
}
