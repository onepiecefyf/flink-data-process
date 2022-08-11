package top.onepiece.model.base;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 数据安全管控平台请求封装
 *
 * @author fengyafei
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RequestHeader {

  private List<String> accept;

  private List<String> acceptEncoding;

  private List<String> acceptLanguage;

  private List<String> contentLength;

  private List<String> contentType;

  private List<String> host;

  private List<String> origin;

  private List<String> referer;

  private List<String> userAgent;
}
