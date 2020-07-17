package org.sunbird.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.models.GroupResponse;

public class GroupUtil {

  /**
   * Update Role details in the group of a user
   *
   * @param groups
   * @param groupRoleMap
   */
  public static void updateRoles(List<GroupResponse> groups, Map<String, String> groupRoleMap) {
    if (!groups.isEmpty()) {
      for (GroupResponse group : groups) {
        group.setMemberRole(groupRoleMap.get(group.getId()));
      }
    }
  }

  public static String convertTimestampToUTC(long timeInMs) {
    Date date = new Date(timeInMs);
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSSZ");
    simpleDateFormat.setLenient(false);
    return simpleDateFormat.format(date);
  }

  public static String convertDateToUTC(Date date) {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSSZ");
    simpleDateFormat.setLenient(false);
    return simpleDateFormat.format(date);
  }

  public static Map<SearchServiceUtil, Map<String, String>> groupActivityIdsBySearchUtilClass(
      List<Map<String, Object>> activities) {
    Map<SearchServiceUtil, Map<String, String>> idClassTypeMap = new HashMap<>();
    for (Map<String, Object> activity : activities) {
      SearchServiceUtil searchUtil =
          ActivityConfigReader.getServiceUtilClassName((String) activity.get(JsonKey.TYPE));
      if (null != searchUtil) {
        if (idClassTypeMap.containsKey(searchUtil)) {
          Map<String, String> idActivityMap = idClassTypeMap.get(searchUtil);
          idActivityMap.put((String) activity.get(JsonKey.ID), (String) activity.get(JsonKey.TYPE));
        } else {
          Map<String, String> idActivityMap = new HashMap<>();
          idActivityMap.put((String) activity.get(JsonKey.ID), (String) activity.get(JsonKey.TYPE));
          idClassTypeMap.put(searchUtil, idActivityMap);
        }
      }
    }
    return idClassTypeMap;
  }
}
