package com.onepiece.flink.dashboard.utils;

/**
 * 字符串处理工具类
 *
 * @author fengyafei
 */
public class StrUtils {

  /**
   * 截取请求路径？之前的URL
   * @param url
   * @return
   */
  public static String subRequestURL(String url) {
    int indexOf = url.indexOf("?");
    if (-1 == indexOf) {
      return url;
    }
    String uri = url.substring(0, indexOf);
    return uri;
  }

  public static void main(String[] args) {
    String uri = subRequestURL("/api/v1/test?id=1&?=5");
    System.out.println(uri);
  }
}
