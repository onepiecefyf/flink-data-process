package top.onepiece.common.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 时间处理工具类
 *
 * @author fengyafei
 */
public class DateUtils {

  /**
   * String类型时间转换为Long类型 时间格式 "yyyy-MM-dd'T'HH:mm:ssss"
   *
   * @param timestamp
   * @param pattern
   * @return
   * @throws Exception
   */
  public static Long transformStringToLong(String timestamp, String pattern) {
    int indexOf = timestamp.lastIndexOf(".");
    String substring = timestamp.substring(0, indexOf + 3);
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
    try {
      Date date = simpleDateFormat.parse(substring);
      return date.getTime();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }
}
