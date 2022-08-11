package com.onepiece.flink.dashboard.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 数据安全管控响应封装
 *
 * @author fengyafei
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DsmpResponse {

  private Long seq;

  private String src;

  private String dest;

  private String timestamp;

  private ResponseHeader header;

  private String body;

  private Integer statusCode;
}
