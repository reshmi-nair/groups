package org.sunbird.util;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This class will contains all the common utility methods.
 *
 * @author Manzarul
 */
public class ProjectUtil {

  public enum Method {
    GET,
    POST,
    PUT,
    DELETE,
    PATCH
  }

  /**
   * This method will provide formatted date
   *
   * @return
   */
  public static String getFormattedDate() {
    return getDateFormatter().format(new Date());
  }

  public static SimpleDateFormat getDateFormatter() {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSSZ");
    simpleDateFormat.setLenient(false);
    return simpleDateFormat;
  }
}
