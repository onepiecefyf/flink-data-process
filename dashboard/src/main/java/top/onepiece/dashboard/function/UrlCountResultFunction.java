package top.onepiece.dashboard.function;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import top.onepiece.model.base.UrlCountView;

/**
 * 统计Url TopN
 *
 * @author fengyafei
 */
public class UrlCountResultFunction implements WindowFunction<Long, UrlCountView, String, TimeWindow> {

  @Override
  public void apply(
      String url, TimeWindow window, Iterable<Long> input, Collector<UrlCountView> collector)
      throws Exception {
    collector.collect(new UrlCountView(url, window.getEnd(), input.iterator().next()));
  }
}
