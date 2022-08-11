package top.onepiece.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import top.onepiece.common.enums.DsmpLogEnum;
import top.onepiece.model.base.Fields;

/**
 * 解析日志json
 *
 * @author fengyafei
 */
public class DashboardAppMapFunction implements MapFunction<String, Tuple2<String, String>> {

  @Override
  public Tuple2<String, String> map(String value) throws Exception {
    JSONObject jsonObject = JSON.parseObject(value);
    Fields fields =
        jsonObject.getObject(DsmpLogEnum.KAFKA_HTTP_DUMP_LOG_FIELD.getField(), Fields.class);
    String message = jsonObject.getString(DsmpLogEnum.KAFKA_HTTP_DUMP_LOG_MESSAGE.getField());
    // 处理requestUri 字段中参数不同而导致分组
    return Tuple2.of(fields.getSource_platform(), message);
  }
}
