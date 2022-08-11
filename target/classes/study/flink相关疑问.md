### 1、分区的作用、实际的应用？


### 2、旁路分支流实际应用？


### 3、滑动窗口解读

#### 3.1、滑动窗口参数  


```java
// 按照url分组
.keyBy(ApacheLogEvent::getUrl)
// 开启10分钟的滑动窗口，每5s更新一次
.timeWindow(Time.minutes(10), Time.seconds(5))
// 允许一分钟的延迟 一分钟之后窗口不在输出结果，窗口关闭，清空状态，迟到的数据放在侧输出流  
.allowedLateness(Time.minutes(1))
.sideOutputLateData(lateTag)
.aggregate(new PageCountAgg(), new PageCountResult());
```

开启另一个定时器，针对延迟数据的处理，延迟数据如果在一分钟之内
```java
// 注册一个1分钟之后的定时器，用来清空状态
ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60 * 1000L);

@Override
public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
    throws Exception {
    // 先判断是否到了窗口关闭清理时间，如果是，直接清空状态返回
    if (timestamp == ctx.getCurrentKey() + 60 * 1000L) {
    pageViewCountMapState.clear();
    return;
    }
}

```


a、假如开启一个滑动窗口10分钟关闭，每5秒钟刷新：意思就是在秒级的时间内，只要是5的倍数就会关闭一个窗口。  
b、 超时数据到来怎样才会再次触发窗口定时器？  
    超时数据再次到来并不会触发定时器，只有当WaterMarker再次更新才会触发定时器  
c、WaterMarker不更新的话，上有任务不会再向下游广播WaterMarker,所以，超时数据的到来不会触发WaterMarker的更新，
只有当前再来一条数据，才会触发WaterMarker更新。

#### 3.2 超时数据处理
此处仅仅是针对窗口的延迟处理，还可以针对排序、计算等设置定时器延迟处理
```java
SingleOutputStreamOperator<PageViewCount> windowAggStream =
    dataStream
        // 过滤get请求
        .filter(data -> "GET".equals(data.getMethod()))
        // 过滤不需要统计的请求URL
        .filter(
            data -> {
              String regex = "^((?!\\.(css|js|png|ico)$).)*$";
              return Pattern.matches(regex, data.getUrl());
            })
        // 按照url分组
        .keyBy(ApacheLogEvent::getUrl)
        // 开启10分钟的滑动窗口，每5s更新一次
        .timeWindow(Time.minutes(10), Time.seconds(5))
        // 允许一分钟的延迟
        .allowedLateness(Time.minutes(1))
        // 延迟数据输出到侧输出流
        .sideOutputLateData(lateTag)
        .aggregate(new PageCountAgg(), new PageCountResult());
```

### 4、全窗口函数
采集一小时之内的数据，直接开启全窗口函数
```java
timeWindowAll(Time.hours(1));
```

1、布隆过滤器  
位图，使用布隆过滤器实现UV(用户浏览量)  












