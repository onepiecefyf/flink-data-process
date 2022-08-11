package top.onepiece.dashboard.tag;

import com.onepiece.flink.dashboard.entity.DsmpRequest;
import org.apache.flink.util.OutputTag;

/**
 * 打标签分流 拆分请求/响应
 * @author fengyafei
 */
public class DashboardOutputTag {

  /**
   * 初始化标签
   * @return
   */
  public static OutputTag<String> buildOutputTag (String tag) {
    OutputTag<String> huuTag = new OutputTag<String>(tag) {};
    return huuTag;
  }

  public static OutputTag<DsmpRequest> buildlateTag (String tag) {
    OutputTag<DsmpRequest> huuTag = new OutputTag<DsmpRequest>(tag) {};
    return huuTag;
  }

}
