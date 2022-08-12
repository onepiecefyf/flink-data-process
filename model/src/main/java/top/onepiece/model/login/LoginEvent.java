package top.onepiece.model.login;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 用户登陆事件
 *
 * @author fengyafei
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginEvent {
  private Long userId;
  private String ip;
  private String status;
  private Long timestamp;
}
