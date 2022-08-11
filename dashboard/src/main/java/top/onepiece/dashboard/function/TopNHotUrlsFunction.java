package top.onepiece.dashboard.function;

import static top.onepiece.common.common.CommonConst.MAP_STATE_DESCRIPTOR;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map.Entry;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import top.onepiece.model.base.UrlCountView;

/**
 * 获取TopN Url
 *
 * @author fengyafei
 */
public class TopNHotUrlsFunction extends KeyedProcessFunction<Long, UrlCountView, String> {

  private Integer topSize;

  public TopNHotUrlsFunction(Integer topSize) {
    this.topSize = topSize;
  }

  /** 定义状态，保存当前所有PageViewCount到Map中 */
  MapState<String, Long> urlViewCountMapState;

  /**
   * 初始化保存url->count map状态
   *
   * @param parameters
   * @throws Exception
   */
  @Override
  public void open(Configuration parameters) throws Exception {
    urlViewCountMapState =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<String, Long>(
                    MAP_STATE_DESCRIPTOR, String.class, Long.class));
  }

  @Override
  public void processElement(
      UrlCountView urlCountView, Context context, Collector<String> collector) throws Exception {
    urlViewCountMapState.put(urlCountView.getUrl(), urlCountView.getCount());
    // 排序定时，需要等待1ms之后开始排序
    context.timerService().registerEventTimeTimer(urlCountView.getWindowEnd() + 1);
    // 注册一个一分钟之后的定时器，用来清空状态
    context.timerService().registerEventTimeTimer(urlCountView.getWindowEnd() + 60 * 1000L);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
    // 先判断是否到了窗口关闭清理时间，如果是，直接清空状态，返回
    if (timestamp == ctx.getCurrentKey() + 60 * 1000L) {
      urlViewCountMapState.clear();
      return;
    }

    ArrayList<Entry<String, Long>> urlViewCounts =
        Lists.newArrayList(urlViewCountMapState.entries());

    urlViewCounts.sort(
        new Comparator<Entry<String, Long>>() {
          @Override
          public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
            if (o1.getValue() > o2.getValue()) {
              return -1;
            } else if (o1.getValue() < o2.getValue()) {
              return 1;
            } else {
              return 0;
            }
          }
        });

    // 格式化成String输出
    StringBuilder resultBuilder = new StringBuilder();
    resultBuilder.append("===================================\n");
    resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

    // 遍历列表，取top n输出
    for (int i = 0; i < Math.min(topSize, urlViewCounts.size()); i++) {
      Entry<String, Long> currentItemViewCount = urlViewCounts.get(i);
      resultBuilder
          .append("NO ")
          .append(i + 1)
          .append(":")
          .append(" 页面URL = ")
          .append(currentItemViewCount.getKey())
          .append(" 浏览量 = ")
          .append(currentItemViewCount.getValue())
          .append("\n");
    }
    resultBuilder.append("===============================\n\n");

    // 控制输出频率
    Thread.sleep(1000L);

    out.collect(resultBuilder.toString());
  }
}
