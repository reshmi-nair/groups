package org.sunbird.actors;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.ActorConfig;
import org.sunbird.exception.BaseException;
import org.sunbird.message.IResponseMessage;
import org.sunbird.message.ResponseCode;
import org.sunbird.models.GroupResponse;
import org.sunbird.request.Request;
import org.sunbird.response.Response;
import org.sunbird.service.GroupService;
import org.sunbird.service.GroupServiceImpl;
import org.sunbird.util.CacheUtil;
import org.sunbird.util.JsonKey;
import org.sunbird.util.JsonUtils;
import org.sunbird.util.helper.PropertiesCache;

@ActorConfig(
  tasks = {"searchGroup"},
  asyncTasks = {},
  dispatcher = "group-dispatcher"
)
public class SearchGroupActor extends BaseActor {

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();
    switch (operation) {
      case "searchGroup":
        searchGroup(request);
        break;

      default:
        onReceiveUnsupportedMessage("SearchGroupActor");
    }
  }

  /**
   * This method will search group related information.
   *
   * @param request
   */
  private void searchGroup(Request request) throws BaseException {
    boolean isUseridRedisEnabled =
        Boolean.parseBoolean(
            PropertiesCache.getInstance().getConfigValue(JsonKey.ENABLE_USERID_REDIS_CACHE));
    CacheUtil cacheUtil = new CacheUtil();
    GroupService groupService = new GroupServiceImpl();
    Map<String, Object> searchQueryMap = request.getRequest();
    logger.info("search group with the request/filters {}", searchQueryMap);
    Map<String, Object> filterMap = (Map<String, Object>) searchQueryMap.get(JsonKey.FILTERS);

    List<GroupResponse> groupDetails = new ArrayList<>();
    String userId = (String) filterMap.get(JsonKey.USER_ID);
    if (StringUtils.isNotBlank(userId)) {
      boolean getFromDB = true;
      if (isUseridRedisEnabled) {
        String groupList = cacheUtil.getCache(userId);
        if (StringUtils.isNotEmpty(groupList)) {
          try {
            groupDetails =
                JsonUtils.deserialize(groupList, new TypeReference<List<GroupResponse>>() {});
            getFromDB = false;
          } catch (Exception e) {
            logger.error("Error in getting group list from Redis: {}", e.getMessage());
          }
        }
      }
      if (getFromDB || CollectionUtils.isEmpty(groupDetails)) {
        groupDetails = groupService.searchGroup(filterMap);
        if (isUseridRedisEnabled) {
          try {
            cacheUtil.setCache(userId, JsonUtils.serialize(groupDetails), CacheUtil.userTtl);
          } catch (Exception e) {
            logger.error("Error in saving group list to Redis: {}", e.getMessage());
          }
        }
      }
    } else {
      logger.error("Bad Request UserId is Mandatory");
      throw new BaseException(
          IResponseMessage.INVALID_REQUESTED_DATA,
          IResponseMessage.MISSING_MANDATORY_PARAMS,
          ResponseCode.BAD_REQUEST.getCode());
    }

    Map<String, Object> result = new HashMap<>();
    if (CollectionUtils.isNotEmpty(groupDetails) && groupDetails.size() > 0) {
      groupDetails.sort(
          ((o1, o2) ->
              o1.getMemberRole().compareTo(o2.getMemberRole()))); // sort group result by role
    }
    result.put(JsonKey.GROUP, groupDetails);
    Response response = new Response(result, ResponseCode.OK.getCode());
    sender().tell(response, self());
  }
}
