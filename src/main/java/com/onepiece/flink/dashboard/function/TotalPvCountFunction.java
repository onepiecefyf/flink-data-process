package com.onepiece.flink.dashboard.function;

import com.onepiece.flink.dashboard.entity.UrlCountView;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 统计总数
 *
 * @author fengyafei
 */
public class TotalPvCountFunction extends KeyedProcessFunction<Long, UrlCountView, UrlCountView> {

  /** 定义状态保存当前值 */
  ValueState<Long> countState;

  @Override
  public void open(Configuration configuration) throws Exception {
    ValueStateDescriptor<Long> valueStateDescriptor =
        new ValueStateDescriptor<Long>(
            "totalCount", TypeInformation.of(new TypeHint<Long>() {}), 0L);
    countState = getRuntimeContext().getState(valueStateDescriptor);
  }

  @Override
  public void processElement(
      UrlCountView urlCountView, Context context, Collector<UrlCountView> collector)
      throws Exception {
    // 来一条数据累加一条
    countState.update(countState.value() + urlCountView.getCount());
    // 注册一个窗口关闭之前1ms的监听器
    context.timerService().registerEventTimeTimer(urlCountView.getWindowEnd() + 1);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<UrlCountView> out)
      throws Exception {
    // 定时器触发，所有分组count都到齐了，直接输出当前count值
    Long count = countState.value();
    out.collect(new UrlCountView("dsmp-pv count: " + count, ctx.getCurrentKey(), count));
    // 清空状态
    countState.clear();
  }
}
