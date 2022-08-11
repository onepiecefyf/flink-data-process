package com.onepiece.flink.dashboard.entity;

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
public class DsmpRequest {

  private Long seq;

  private String src;

  private String dest;

  private String timestamp;

  private String requestUri;

  private String method;

  private String host;

  private RequestHeader header;

  private String body;

  private String appName;

  @Override
  public String toString() {
    return "DsmpRequest{" +
        "seq=" + seq +
        ", src='" + src + '\'' +
        ", dest='" + dest + '\'' +
        ", timestamp='" + timestamp + '\'' +
        ", requestUri='" + requestUri + '\'' +
        ", method='" + method + '\'' +
        ", host='" + host + '\'' +
        ", header=" + header +
        ", body='" + body + '\'' +
        ", appName='" + appName + '\'' +
        '}';
  }
}
