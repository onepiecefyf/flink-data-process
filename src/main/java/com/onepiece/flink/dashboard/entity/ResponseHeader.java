package com.onepiece.flink.dashboard.entity;

import java.util.List;
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
public class ResponseHeader {

  private List<String> connection;

  private List<String> contentLength;

  private List<String> contentType;

  private List<String> date;

  private List<String> server;
}
