package com.onepiece.flink.demo.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/** @author fengyafei */
public class MyNoParalleSource implements SourceFunction<String> {

  private boolean isRunning = true;

  /**
   * 主要的方法
   *
   * <p>启动一个source
   *
   * <p>大部分情况下，都需要在这个run方法中实现一个循环，这样就可以循环产生数据了
   *
   * @param ctx
   * @throws Exception
   */
  @Override
  public void run(SourceContext<String> ctx) throws Exception {

    while (isRunning) {

      // 图书的排行榜

      List<String> books = new ArrayList<>();

      books.add("Pyhton从入门到放弃");

      books.add("Java从入门到放弃");

      books.add("Php从入门到放弃");

      books.add("C++从入门到放弃");

      books.add("Scala从入门到放弃");

      int i = new Random().nextInt(5);

      ctx.collect(books.get(i));

      // 每2秒产生一条数据

      Thread.sleep(2000);
    }
  }

  // 取消一个cancel的时候会调用的方法

  @Override
  public void cancel() {

    isRunning = false;
  }
}
