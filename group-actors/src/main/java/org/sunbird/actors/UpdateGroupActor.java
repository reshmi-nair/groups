package org.sunbird.actors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.ActorConfig;
import org.sunbird.exception.BaseException;
import org.sunbird.message.IResponseMessage;
import org.sunbird.message.ResponseCode;
import org.sunbird.models.Group;
import org.sunbird.models.MemberResponse;
import org.sunbird.request.Request;
import org.sunbird.response.Response;
import org.sunbird.service.GroupService;
import org.sunbird.service.GroupServiceImpl;
import org.sunbird.service.MemberService;
import org.sunbird.service.MemberServiceImpl;
import org.sunbird.telemetry.TelemetryEnvKey;
import org.sunbird.telemetry.util.TelemetryUtil;
import org.sunbird.util.CacheUtil;
import org.sunbird.util.GroupRequestHandler;
import org.sunbird.util.GroupUtil;
import org.sunbird.util.JsonKey;
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

    String userId = group.getUpdatedBy();
    if(StringUtils.isEmpty(userId)){
      throw new BaseException(
              IResponseMessage.Key.UNAUTHORIZED_USER,
              IResponseMessage.Message.UNAUTHORIZED_USER,
              ResponseCode.CLIENT_ERROR.getCode());
    }

    Map responseMap = null;
    if (MapUtils.isNotEmpty((Map) actorMessage.getRequest().get(JsonKey.MEMBERS))) {
      // member updates to group
      responseMap = handleMemberOperation(
              group.getId(),
              (Map) actorMessage.getRequest().get(JsonKey.MEMBERS),
              requestHandler.getRequestedBy(actorMessage));
    }

    // Group and activity updates
    handleGroupActivityOperation(
        group, (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ACTIVITIES));

    Response response = new Response(ResponseCode.OK.getCode());
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    if(MapUtils.isNotEmpty(responseMap)){
      response.put(JsonKey.ERROR, responseMap);
    }
    sender().tell(response, self());

    logTelemetry(actorMessage, group);
  }

  private void handleGroupActivityOperation(Group group, Map<String, Object> activityOperationMap) {
    GroupService groupService = new GroupServiceImpl();
    Integer totalActivityCount = 0;
    if (MapUtils.isNotEmpty(activityOperationMap)) {
      List<Map<String, Object>> updateActivityList =
            groupService.handleActivityOperations(group.getId(), activityOperationMap);
        totalActivityCount = updateActivityList.size();

      GroupUtil.checkMaxActivityLimit(totalActivityCount);
      cacheUtil.delCache(group.getId());
      group.setActivities(updateActivityList);
    }
    Response response = groupService.updateGroup(group);
  }

  private Map handleMemberOperation(String groupId, Map memberOperationMap, String requestedBy) {
    Map validationErrors = new HashMap<>();
    List errorList = new ArrayList();
    validationErrors.put("members",errorList);

    MemberService memberService = new MemberServiceImpl();
    List<MemberResponse> membersInDB = memberService.fetchMembersByGroupId(groupId);

    GroupRequestHandler requestHandler = new GroupRequestHandler();
    //Validate Member Addition
    if(CollectionUtils.isNotEmpty((List<Map<String, Object>>) memberOperationMap.get(JsonKey.ADD))) {
      requestHandler.validateAddMembers(memberOperationMap, membersInDB, validationErrors);
    }
    //Validate Member Update
    if(CollectionUtils.isNotEmpty((List<Map<String, Object>>) memberOperationMap.get(JsonKey.EDIT))) {
      requestHandler.validateEditMembers(memberOperationMap, membersInDB, validationErrors);
    }
    //Validate Member Remove
    if(CollectionUtils.isNotEmpty((List<String>) memberOperationMap.get(JsonKey.REMOVE))){
      requestHandler.validateRemoveMembers(memberOperationMap,  membersInDB, validationErrors);
    }
    int totalMemberCount = GroupUtil.totalMemberCount(memberOperationMap, membersInDB);
    GroupUtil.checkMaxMemberLimit(totalMemberCount);

    boolean isUseridRedisEnabled =
        Boolean.parseBoolean(
            PropertiesCache.getInstance().getConfigValue(JsonKey.ENABLE_USERID_REDIS_CACHE));
    if (isUseridRedisEnabled) {
      //Remove group list user cache from redis
      deleteUserCache(memberOperationMap);
    }
    cacheUtil.delCache(groupId + "_" + JsonKey.MEMBERS);
    memberService.handleMemberOperations(memberOperationMap, groupId, requestedBy);

    return validationErrors;
  }

  private void deleteUserCache(Map memberOperationMap) {
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

  private void logTelemetry(Request actorMessage, Group group){
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

}
