package com.onepiece.flink.common;

/**
 * 通用常量
 * @author fengyafei
 */
public interface CommonConst {

  String DSMP_URL_TOP = "dsmp-url-top";
  String DSMP_PV_JOB = "dsmp-pv-job";

  String KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";
  String KAFKA_IP_PORT = "192.168.112.50:9092";
  String KAFKA_TOP_GROUP_ID = "dsmp-top-n";
  String KAFKA_PV_GROUP_ID = "dsmp-pv";
  String KAFKA_GROUP_ID = "group_id";
  String KAFKA_HTTP_DUMP_TOPIC = "httpdumpTopic";
  String LOG_REQUEST_RESPONSE_SPLIT = "huu";
  String LATE_TAG = "dsmp_request_late";
  String LOG_REQUEST_METHOD = "method";
  String LOG_APP_NAME = "appName";
  String APP_DSMP = "dsmp";
  String DATA_FORMAT = "yyyy-MM-dd'T'HH:mm:ssss";
  String MAP_STATE_DESCRIPTOR = "dsmp-url-top-count";
  String DASH_BOARD_OUT_TAG_RESPONSE = "login-fail-out-tag";
}
