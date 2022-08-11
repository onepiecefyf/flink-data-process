package com.onepiece.flink.demo.source;

import com.onepiece.flink.demo.entity.Event;
import java.util.Calendar;
import java.util.Random;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义数据来源
 *
 * @author fengyafei
 */
public class ClickSource implements SourceFunction<Event> {

  /**
   * 控制数据生成的标识位
   */
  private Boolean running = true;

  /**
   * 使用运行时上下文对象（SourceContext）向下游发送数据
   *
   * @param sourceContext
   * @throws Exception
   */
  @Override
  public void run(SourceContext sourceContext) throws Exception {
    Random random = new Random();
    String[] users = {"Mary", "Alice", "Bob", "Cary"};
    String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

    while (running) {
      sourceContext.collect(
          new Event(
              users[random.nextInt(users.length)],
              urls[random.nextInt(urls.length)],
              Calendar.getInstance().getTimeInMillis()));
      // 隔 1 秒生成一个点击事件，方便观测
      Thread.sleep(1000);
    }
  }

  /** 通过标识位控制退出循环，来达到中断数据源的效果 */
  @Override
  public void cancel() {
    running = false;
  }
}
