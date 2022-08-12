package top.onepiece.login.function;

import java.util.List;
import java.util.Map;
import org.apache.flink.cep.PatternSelectFunction;
import top.onepiece.model.login.LoginEvent;
import top.onepiece.model.login.LoginFailAlertMsg;

/**
 * CEP模式匹配登陆失败
 *
 * @author fengyafei
 */
public class LoginFailPatternAlertFunction
    implements PatternSelectFunction<LoginEvent, LoginFailAlertMsg> {

  @Override
  public LoginFailAlertMsg select(Map<String, List<LoginEvent>> pattern) throws Exception {
    LoginEvent firstFailEvent = pattern.get("failEvents").get(0);
    LoginEvent lastFailEvent = pattern.get("failEvents").get(pattern.get("failEvents").size() - 1);
    return new LoginFailAlertMsg(
        firstFailEvent.getUserId(),
        firstFailEvent.getTimestamp(),
        lastFailEvent.getTimestamp(),
        "login fail " + pattern.get("failEvents").size() + " times");
  }
}
