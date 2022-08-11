package top.onepiece.model.base;

import java.sql.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Url TopN
 * @author fengyafei
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UrlCountView {
  private String url;
  private Long windowEnd;
  private Long count;

  @Override
  public String toString() {
    return "UrlCountView{" +
        "url='" + url + '\'' +
        ", windowEnd=" + new Timestamp(windowEnd) +
        ", count=" + count +
        '}';
  }
}
