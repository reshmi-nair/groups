package org.sunbird.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.response.Response;
import org.sunbird.service.UserService;
import org.sunbird.service.UserServiceImpl;
import org.sunbird.util.helper.PropertiesCache;

public class SystemConfigUtil {
  private static Logger logger = LoggerFactory.getLogger(SystemConfigUtil.class);
  private static UserService userService = UserServiceImpl.getInstance();
  private static Map<String, Object> custodianOrgDetails = new HashMap<>();
  private static Integer MAX_GROUP_MEMBER_LIMIT;
  private static Integer MAX_ACTIVITY_LIMIT;

  public static void init() {

    cacheCustodianOrgDetails();
    String maxGroupMemberLimit = PropertiesCache.getConfigValue(JsonKey.MAX_GROUP_MEMBERS_LIMIT);
    String maxActivityLimit = PropertiesCache.getConfigValue(JsonKey.MAX_ACTIVITY_LIMIT);
    if (StringUtils.isNotBlank(maxGroupMemberLimit)) {
      MAX_GROUP_MEMBER_LIMIT = Integer.parseInt(maxGroupMemberLimit);
    } else {
      MAX_GROUP_MEMBER_LIMIT =
          Integer.parseInt(PropertiesCache.getConfigValue(JsonKey.MAX_GROUP_MEMBERS_LIMIT));
    }
    if (StringUtils.isNotBlank(maxGroupMemberLimit)) {
      MAX_ACTIVITY_LIMIT = Integer.parseInt(maxActivityLimit);
    } else {
      MAX_ACTIVITY_LIMIT =
          Integer.parseInt(PropertiesCache.getConfigValue(JsonKey.MAX_ACTIVITY_LIMIT));
    }
  }

  private static Map<String, String> getSystemSettingConfig() {
    Map<String, String> systemSettingConfig = new HashMap<>();
    Response response = userService.getSystemSettings();
    logger.info(
        "DataCacheHandler:cacheSystemConfig: Cache system setting fields" + response.getResult(),
        LoggerEnum.INFO.name());
    List<Map<String, Object>> responseList =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (null != responseList && !responseList.isEmpty()) {
      for (Map<String, Object> resultMap : responseList) {
        systemSettingConfig.put(
            ((String) resultMap.get(JsonKey.FIELD)), (String) resultMap.get(JsonKey.VALUE));
      }
    }

    return systemSettingConfig;
  }

  private static void cacheCustodianOrgDetails() {
    Map<String, String> configSettings = getSystemSettingConfig();
    String custodianOrgId = configSettings.get(JsonKey.CUSTODIAN_ORG_ID);
    if (null != custodianOrgId) {
      Response response = userService.getOrganisationDetails(custodianOrgId);
      logger.info(
          "DataCacheHandler:cacheSystemConfig: Cache system setting fields" + response.getResult(),
          LoggerEnum.INFO.name());
      if (null != response
          && null != response.getResult()
          && null != response.getResult().get(JsonKey.RESPONSE)) {
        custodianOrgDetails = (Map<String, Object>) response.getResult().get(JsonKey.RESPONSE);
      }
    }
  }

  public static Map<String, Object> getCustodianOrgDetails() {
    return custodianOrgDetails;
  }

  public static Integer getMaxActivityLimit() {
    return MAX_ACTIVITY_LIMIT;
  }

  public static Integer getMaxGroupMemberLimit() {
    return MAX_GROUP_MEMBER_LIMIT;
  }
}
