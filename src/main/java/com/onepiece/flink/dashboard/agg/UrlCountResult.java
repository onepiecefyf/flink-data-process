package com.onepiece.flink.dashboard.agg;

import com.onepiece.flink.dashboard.entity.UrlCountView;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 统计Url TopN
 *
 * @author fengyafei
 */
public class UrlCountResult implements WindowFunction<Long, UrlCountView, String, TimeWindow> {

  @Override
  public void apply(
      String url, TimeWindow window, Iterable<Long> input, Collector<UrlCountView> collector)
      throws Exception {
    collector.collect(new UrlCountView(url, window.getEnd(), input.iterator().next()));
  }
}
