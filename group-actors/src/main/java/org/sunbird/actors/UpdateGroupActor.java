package org.sunbird.actors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.ActorConfig;
import org.sunbird.exception.BaseException;
import org.sunbird.message.ResponseCode;
import org.sunbird.models.Group;
import org.sunbird.request.Request;
import org.sunbird.response.Response;
import org.sunbird.service.GroupService;
import org.sunbird.service.GroupServiceImpl;
import org.sunbird.service.MemberService;
import org.sunbird.service.MemberServiceImpl;
import org.sunbird.telemetry.TelemetryEnvKey;
import org.sunbird.telemetry.util.TelemetryUtil;
import org.sunbird.util.*;
import org.sunbird.util.helper.PropertiesCache;

@ActorConfig(
  tasks = {"updateGroup"},
  asyncTasks = {},
  dispatcher = "group-dispatcher"
)
public class UpdateGroupActor extends BaseActor {
  private CacheUtil cacheUtil = new CacheUtil();

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();
    switch (operation) {
      case "updateGroup":
        updateGroup(request);
        break;
      default:
        onReceiveUnsupportedMessage("UpdateGroupActor");
    }
  }
  /**
   * This method will update group in cassandra based on group id.
   *
   * @param actorMessage
   */
  private void updateGroup(Request actorMessage) throws BaseException {
    logger.info("UpdateGroup method call");
    GroupRequestHandler requestHandler = new GroupRequestHandler();
    Group group = requestHandler.handleUpdateGroupRequest(actorMessage);
    logger.info("Update group for the groupId {}", group.getId());

    // member updates to group
    handleMemberOperation(
        group.getId(),
        (Map) actorMessage.getRequest().get(JsonKey.MEMBERS),
        requestHandler.getRequestedBy(actorMessage));

    // Group and activity updates
    handleGroupActivityOperation(
        group, (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ACTIVITIES));

    Response response = new Response(ResponseCode.OK.getCode());
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    sender().tell(response, self());
    Map<String, Object> targetObject = null;
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    if (null != group.getStatus() && JsonKey.INACTIVE.equals(group.getStatus())) {
      targetObject =
          TelemetryUtil.generateTargetObject(
              group.getId(), TelemetryEnvKey.GROUP, JsonKey.DELETE, null);
    } else {
      targetObject =
          TelemetryUtil.generateTargetObject(
              group.getId(), TelemetryEnvKey.GROUP, JsonKey.UPDATE, null);
    }
    TelemetryUtil.telemetryProcessingCall(
        actorMessage.getRequest(), targetObject, correlatedObject, actorMessage.getContext());
  }

  private void handleGroupActivityOperation(Group group, Map<String, Object> activityOperationMap) {
    GroupService groupService = new GroupServiceImpl();
    Integer totalActivityCount = 0;
    if (MapUtils.isNotEmpty(activityOperationMap)) {
      String groupActivityCount =
          cacheUtil.getCache(group.getId() + "_" + JsonKey.ACTIVITIES + "_" + JsonKey.COUNT);
      List<Map<String, Object>> updateActivityList = new ArrayList<>();
      if (StringUtils.isNotBlank(groupActivityCount)) {
        totalActivityCount = Integer.parseInt(groupActivityCount);

        List<Map<String, Object>> activityAddList =
            (List<Map<String, Object>>) activityOperationMap.get(JsonKey.ADD);
        List<String> activityRemoveList = (List<String>) activityOperationMap.get(JsonKey.REMOVE);

        if (CollectionUtils.isNotEmpty(activityAddList)) {
          totalActivityCount += activityAddList.size();
        }
        if (CollectionUtils.isNotEmpty(activityRemoveList)) {
          totalActivityCount -= activityRemoveList.size();
        }
      } else {
        updateActivityList =
            groupService.handleActivityOperations(group.getId(), activityOperationMap);
        totalActivityCount = updateActivityList.size();
      }
      GroupUtil.checkMaxActivityLimit(totalActivityCount);
      cacheUtil.delCache(group.getId() + "_" + JsonKey.ACTIVITIES + "_" + JsonKey.COUNT);
      cacheUtil.delCache(group.getId());
      group.setActivities(updateActivityList);
    }

    Response response = groupService.updateGroup(group);

    // update only when activity changes
    if (MapUtils.isNotEmpty(activityOperationMap)) {
      cacheUtil.setCache(
          group.getId() + "_" + JsonKey.ACTIVITIES + "_" + JsonKey.COUNT,
          String.valueOf(totalActivityCount),
          CacheUtil.groupTtl);
    }
  }

  private void handleMemberOperation(String groupId, Map memberOperationMap, String requestedBy) {

    MemberService memberService = new MemberServiceImpl();

    if (MapUtils.isNotEmpty(memberOperationMap)) {
      List<Map<String, Object>> memberAddList =
          (List<Map<String, Object>>) memberOperationMap.get(JsonKey.ADD);
      List<String> memberRemoveList = (List<String>) memberOperationMap.get(JsonKey.REMOVE);
      String groupMemberCount =
          cacheUtil.getCache(groupId + "_" + JsonKey.MEMBERS + "_" + JsonKey.COUNT);
      Integer groupMemberCurrentCount;
      if (StringUtils.isNotBlank(groupMemberCount)) {
        groupMemberCurrentCount = Integer.parseInt(groupMemberCount);
      } else {
        groupMemberCurrentCount = memberService.fetchMemberSize(groupId);
      }
      int totalMemberCount =
          groupMemberCurrentCount
              + (null != memberAddList ? memberAddList.size() : 0)
              - (null != memberRemoveList ? memberRemoveList.size() : 0);

      GroupUtil.checkMaxMemberLimit(totalMemberCount);
      boolean isUseridRedisEnabled =
          Boolean.parseBoolean(
              PropertiesCache.getInstance().getConfigValue(JsonKey.ENABLE_USERID_REDIS_CACHE));
      if (isUseridRedisEnabled) {
        deleteUserCache(memberOperationMap);
      }
      cacheUtil.delCache(groupId + "_" + JsonKey.MEMBERS + "_" + JsonKey.COUNT);
      cacheUtil.delCache(groupId + "_" + JsonKey.MEMBERS);
      memberService.handleMemberOperations(memberOperationMap, groupId, requestedBy);
      cacheUtil.setCache(
          groupId + "_" + JsonKey.MEMBERS + "_" + JsonKey.COUNT,
          String.valueOf(totalMemberCount),
          CacheUtil.groupTtl);
    }
  }

  public void deleteUserCache(Map memberOperationMap) {
    List<Map<String, Object>> memberAddList =
        (List<Map<String, Object>>) memberOperationMap.get(JsonKey.ADD);
    if (CollectionUtils.isNotEmpty(memberAddList)) {
      memberAddList.forEach(member -> cacheUtil.delCache((String) (member.get(JsonKey.USER_ID))));
    }
    List<Map<String, Object>> memberEditList =
        (List<Map<String, Object>>) memberOperationMap.get(JsonKey.EDIT);
    if (CollectionUtils.isNotEmpty(memberEditList)) {
      memberEditList.forEach(member -> cacheUtil.delCache((String) (member.get(JsonKey.USER_ID))));
    }
    List<String> memberRemoveList = (List<String>) memberOperationMap.get(JsonKey.REMOVE);
    if (CollectionUtils.isNotEmpty(memberRemoveList)) {
      memberRemoveList.forEach(member -> cacheUtil.delCache(member));
    }
  }
}
