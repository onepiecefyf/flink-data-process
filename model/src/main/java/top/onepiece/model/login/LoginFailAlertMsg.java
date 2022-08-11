package top.onepiece.model.login;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 告警信息
 * @author fengyafei
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginFailAlertMsg {
    /** 用户ID */
    private Long userId;
    /** 用户第一次登陆失败时间 */
    private Long firstFailTime;
    /** 用户最后登陆失败时间 */
    private Long lastFailTime;
    /** 警告信息 */
    private String warningMsg;
}
