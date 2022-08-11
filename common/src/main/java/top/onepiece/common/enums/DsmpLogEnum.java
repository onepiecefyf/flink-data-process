package top.onepiece.common.enums;

import org.apache.commons.lang3.StringUtils;

public enum DsmpLogEnum {

  KAFKA_HTTP_DUMP_LOG_FIELD("fields", "字段名称"),
  KAFKA_HTTP_DUMP_LOG_MESSAGE("message", "字段名称");

  private String field;
  private String message;


  DsmpLogEnum(String field, String message) {
    this.field = field;
    this.message = message;
  }

  public static String getMessage(String code) {
    for (DsmpLogEnum r : DsmpLogEnum.values()) {
      if (r.field == code) {
        return r.message;
      }
    }
    return StringUtils.EMPTY;
  }

  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }



}
