package org.sunbird.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.exception.BaseException;
import org.sunbird.message.IResponseMessage;
import org.sunbird.message.ResponseCode;
import org.sunbird.models.GroupResponse;

public class GroupUtil {

  private static Logger logger = LoggerFactory.getLogger(GroupUtil.class);
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

  public static void checkMaxActivityLimit(Integer totalActivityCount) {
    if (totalActivityCount > SystemConfigUtil.getMaxActivityLimit()) {
      logger.error("List of activities exceeded the activity size limit:{}", totalActivityCount);
      throw new BaseException(
          IResponseMessage.EXCEEDED_MAX_LIMIT,
          IResponseMessage.Message.EXCEEDED_ACTIVITY_MAX_LIMIT,
          ResponseCode.CLIENT_ERROR.getCode());
    }
  }

  public static void checkMaxMemberLimit(int totalMemberCount) {
    if (totalMemberCount > SystemConfigUtil.getMaxGroupMemberLimit()) {
      logger.error("List of members exceeded the member size limit:{}", totalMemberCount);
      throw new BaseException(
          IResponseMessage.EXCEEDED_MAX_LIMIT,
          IResponseMessage.Message.EXCEEDED_MEMBER_MAX_LIMIT,
          ResponseCode.CLIENT_ERROR.getCode());
    }
  }
}
