package top.onepiece.example.demo.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 事件基本类
 * 1、类必须是public(公有)
 * 2、必须包含一个无参的构造方法
 * 3、所有属性必须是公有的
 * 4、所有属性类型必须可以序列化
 *
 * @author fengyafei
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Event {
  public String user;
  public String url;
  public long timestamp;

  @Override
  public String toString() {
    return "Event{"
        + "user='"
        + user
        + '\''
        + ", url='"
        + url
        + '\''
        + ", timestamp="
        + timestamp
        + '}';
  }
}
