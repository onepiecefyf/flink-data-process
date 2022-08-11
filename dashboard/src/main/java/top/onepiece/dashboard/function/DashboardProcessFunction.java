package top.onepiece.dashboard.function;

import static com.onepiece.flink.common.CommonConst.LOG_REQUEST_METHOD;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 解析请求参数
 *
 * @author fengyafei
 */
public class DashboardProcessFunction extends ProcessFunction<Tuple2<String, String>, String> {

  private OutputTag<String> outputTag;

  public DashboardProcessFunction() {

  }

  public DashboardProcessFunction(OutputTag<String> outputTag) {
    this.outputTag = outputTag;
  }

  @Override
  public void processElement(Tuple2<String, String> tuple, Context ctx, Collector<String> out)
      throws Exception {
    String message = tuple.f1;
    JSONObject jsonObject = JSON.parseObject(message);
    if (jsonObject.containsKey(LOG_REQUEST_METHOD)) {
      // 请求流
      String newMessage = jsonObject.toJSONString();
      out.collect(newMessage);
    } else if (null != outputTag){
      // 响应流
      String newMessage = jsonObject.toString();
      ctx.output(outputTag, newMessage);
    }
  }
}
