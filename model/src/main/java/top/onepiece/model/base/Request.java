package top.onepiece.model.base;

import java.util.List;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @Description:
 * @author: zhouwubiao@bjca.org.cn
 * @date: Create in  2022/5/9
 */
@lombok.NoArgsConstructor
@lombok.Data
public class Request {

  @JsonProperty("seq")
  private Long seq;
  @JsonProperty("src")
  private String src;
  @JsonProperty("dest")
  private String dest;
  @JsonProperty("timestamp")
  private String timestamp;
  @JsonProperty("requestUri")
  private String requestUri;
  @JsonProperty("method")
  private String method;
  @JsonProperty("host")
  private String host;
  @JsonProperty("header")
  private HeaderDTO header;
  @JsonProperty("body")
  private String body;
  @JsonProperty("appName")
  private String appName;


  @lombok.NoArgsConstructor
  @lombok.Data
  public static class HeaderDTO {

    @JsonProperty("Accept")
    private List<String> accept;
    @JsonProperty("Accept-Encoding")
    private List<String> acceptEncoding;
    @JsonProperty("Accept-Language")
    private List<String> acceptLanguage;
    @JsonProperty("Content-Length")
    private List<String> contentLength;
    @JsonProperty("Content-Type")
    private List<String> contentType;
    @JsonProperty("Host")
    private List<String> host;
    @JsonProperty("Origin")
    private List<String> origin;
    @JsonProperty("Referer")
    private List<String> referer;
    @JsonProperty("User-Agent")
    private List<String> userAgent;
  }


}
