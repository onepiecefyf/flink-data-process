package com.onepiece.flink.entity;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @Description:
 *
 * @author: zhouwubiao@bjca.org.cn
 * @date: Create in 2022/6/14
 */
@Data
public class Fields {
  @JsonProperty("source_platform")
  private String source_platform;

  @JsonProperty("log_topic")
  private String log_topic;
}
