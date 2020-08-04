package org.sunbird.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.exception.BaseException;
import org.sunbird.message.IResponseMessage;
import org.sunbird.message.ResponseCode;
import org.sunbird.models.GroupResponse;
import org.sunbird.models.Member;
import org.sunbird.models.MemberResponse;
import org.sunbird.util.helper.PropertiesCache;

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
    logger.info("groupActivityIdsBySearchUtilClass");
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

  public static void checkMaxGroupLimit(List<Map<String, Object>> userGroupsList, String userId) {
    int groupCount = 0;
    if(CollectionUtils.isNotEmpty(userGroupsList)) {
      Map<String, Object> userInfo = userGroupsList
              .stream()
              .filter(userMap -> userId.equals((String) userMap.get(JsonKey.USER_ID)))
              .findFirst()
              .orElse(null);
      if (MapUtils.isNotEmpty(userInfo)) {
        groupCount = ((Set<String>) userInfo.get(JsonKey.GROUP_ID)).size();
      }
    }
    int maxGroupLimit = Integer.parseInt(PropertiesCache.getInstance().getProperty(JsonKey.MAX_GROUP_LIMIT));
    if (groupCount > maxGroupLimit) {
      logger.error("List of groups exceeded the max limit:{}", groupCount);
      throw new BaseException(
              IResponseMessage.EXCEEDED_MAX_LIMIT,
              IResponseMessage.Message.EXCEEDED_GROUP_MAX_LIMIT,
              ResponseCode.CLIENT_ERROR.getCode());
    }
  }

  public static void checkMaxActivityLimit(Integer totalActivityCount) {
    int activityLimit = Integer.parseInt(PropertiesCache.getInstance().getProperty(JsonKey.MAX_ACTIVITY_LIMIT));
    if (totalActivityCount > activityLimit) {
      logger.error("List of activities exceeded the activity size limit:{}", totalActivityCount);
      throw new BaseException(
          IResponseMessage.EXCEEDED_MAX_LIMIT,
          IResponseMessage.Message.EXCEEDED_ACTIVITY_MAX_LIMIT,
          ResponseCode.CLIENT_ERROR.getCode());
    }
  }

  public static void checkMaxMemberLimit(int totalMemberCount) {
    int memberLimit = Integer.parseInt(PropertiesCache.getInstance().getProperty(JsonKey.MAX_GROUP_MEMBERS_LIMIT));
    if (totalMemberCount > memberLimit) {
      logger.error("List of members exceeded the member size limit:{}", totalMemberCount);
      throw new BaseException(
          IResponseMessage.EXCEEDED_MAX_LIMIT,
          IResponseMessage.Message.EXCEEDED_MEMBER_MAX_LIMIT,
          ResponseCode.CLIENT_ERROR.getCode());
    }
  }

  public static int totalMemberCount(Map memberOperationMap, List<MemberResponse> membersInDB){
    int totalMemberCount = (null != membersInDB ? membersInDB.size() : 0);

    List<Map<String, Object>> memberAddList = (List<Map<String, Object>>) memberOperationMap.get(JsonKey.ADD);
    if (CollectionUtils.isNotEmpty(memberAddList)) {
      totalMemberCount += memberAddList.size();
    }

    List<Map<String, Object>> memberRemoveList = (List<Map<String, Object>>) memberOperationMap.get(JsonKey.REMOVE);
    if (CollectionUtils.isNotEmpty(memberRemoveList)) {
      totalMemberCount -= memberRemoveList.size();
    }
    return totalMemberCount;
  }

  public static List<String> getMemberIdList(List<Member> member){
    List<String> members =
            member
                    .stream()
                    .map(data -> data.getUserId())
                    .collect(Collectors.toList());
    return members;
  }

  public static List<String> getMemberIdListFromMap( List<Map<String, Object>> member){
    List<String> members =
            member
                    .stream()
                    .map(data -> (String) data.get(JsonKey.USER_ID))
                    .collect(Collectors.toList());
    return members;
  }
}
