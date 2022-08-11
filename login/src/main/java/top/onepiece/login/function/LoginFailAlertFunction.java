package top.onepiece.login.function;

import static top.onepiece.common.common.CommonConst.DATA_FORMAT;
import static top.onepiece.common.common.CommonConst.LOGIN_FAIL_FIELD;

import java.util.Iterator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import top.onepiece.common.utils.DateUtils;
import top.onepiece.model.base.DsmpResponse;
import top.onepiece.model.login.LoginFailAlertMsg;

/**
 * 用户三秒內登陆失败超过两次告警处理方法
 *
 * @author fengyafei
 */
public class LoginFailAlertFunction
    extends KeyedProcessFunction<Long, DsmpResponse, LoginFailAlertMsg> {

  /** 两秒内允许登陆失败的最大次数 */
  private Integer maxLoginFailCount;
  /** 保存两秒内所有登陆失败的次数 */
  private ListState<DsmpResponse> loginFailAlertListState;

  public LoginFailAlertFunction(Integer maxLoginFailCount) {
    this.maxLoginFailCount = maxLoginFailCount;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    loginFailAlertListState =
        getRuntimeContext()
            .getListState(
                new ListStateDescriptor<DsmpResponse>("login-fail-function", DsmpResponse.class));
  }

  @Override
  public void processElement(
      DsmpResponse response, Context context, Collector<LoginFailAlertMsg> collector)
      throws Exception {
    // 如果是登陆失败
    if (LOGIN_FAIL_FIELD.equals(response.getBody())) {
      Iterator<DsmpResponse> iterator = loginFailAlertListState.get().iterator();
      if (iterator.hasNext()) {
        // 如果已经存在登陆失败的时间 判断是否在两秒内
        DsmpResponse dsmpResponse = iterator.next();
        Long oldLoginTime =
            DateUtils.transformStringToLong(dsmpResponse.getTimestamp(), DATA_FORMAT);
        Long currentLoginTime =
            DateUtils.transformStringToLong(response.getTimestamp(), DATA_FORMAT);

        // 如果在两秒内 直接告警
        if (oldLoginTime - currentLoginTime <= maxLoginFailCount) {
          collector.collect(new LoginFailAlertMsg(1L, oldLoginTime, currentLoginTime, ""));
        }

        // 本次处理结束 更新状态
        loginFailAlertListState.clear();
        loginFailAlertListState.add(response);
      } else {
        // 没有登陆失败 直接放入列表
        loginFailAlertListState.add(response);
      }
    } else {
      // 登陆成功 直接清除
      loginFailAlertListState.clear();
    }
  }
}
