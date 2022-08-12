package top.onepiece.dashboard.tag;

import org.apache.flink.util.OutputTag;
import top.onepiece.model.base.DsmpRequest;

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
