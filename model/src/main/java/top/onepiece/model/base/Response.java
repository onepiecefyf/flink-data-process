package top.onepiece.model.base;

import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @Description:
 * @author: zhouwubiao@bjca.org.cn
 * @date: Create in  2022/5/10
 */
@NoArgsConstructor
@Data
public class Response {

  @JsonProperty("seq")
  private Long seq;
  @JsonProperty("src")
  private String src;
  @JsonProperty("dest")
  private String dest;
  @JsonProperty("timestamp")
  private String timestamp;
  @JsonProperty("header")
  private HeaderDTO header;
  @JsonProperty("body")
  private String body;
  @JsonProperty("statusCode")
  private Integer statusCode;

  @NoArgsConstructor
  @Data
  public static class HeaderDTO {

    @JsonProperty("Connection")
    private List<String> connection;
    @JsonProperty("Content-Length")
    private List<String> contentLength;
    @JsonProperty("Content-Type")
    private List<String> contentType;
    @JsonProperty("Date")
    private List<String> date;
    @JsonProperty("Server")
    private List<String> server;
  }


}
