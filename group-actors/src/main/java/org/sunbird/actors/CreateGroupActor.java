package org.sunbird.actors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.ActorConfig;
import org.sunbird.exception.BaseException;
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
  tasks = {"createGroup"},
  asyncTasks = {},
  dispatcher = "group-dispatcher"
)
public class CreateGroupActor extends BaseActor {

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();
    switch (operation) {
      case "createGroup":
        createGroup(request);
        break;
      default:
        onReceiveUnsupportedMessage("CreateGroupActor");
    }
  }
  /**
   * This method will create group in cassandra.
   *
   * @param actorMessage
   */
  private void createGroup(Request actorMessage) throws BaseException {
    GroupService groupService = new GroupServiceImpl();
    MemberService memberService = new MemberServiceImpl();

    GroupRequestHandler requestHandler = new GroupRequestHandler();
    Group group = requestHandler.handleCreateGroupRequest(actorMessage);

    // add creator of group to memberList as admin
    List<Map<String, Object>> memberList = new ArrayList<>();
    Map<String, Object> createdUser = new HashMap<>();
    createdUser.put(JsonKey.USER_ID, requestHandler.getRequestedBy(actorMessage));
    createdUser.put(JsonKey.ROLE, JsonKey.ADMIN);
    memberList.add(createdUser);

    // adding members to group, if members are provided in request
    List<Map<String, Object>> reqMemberList =
        (List<Map<String, Object>>) actorMessage.getRequest().get(JsonKey.MEMBERS);
    if (CollectionUtils.isNotEmpty(reqMemberList)) {
      memberList.addAll(reqMemberList);
    }

    GroupUtil.checkMaxMemberLimit(memberList.size());
    GroupUtil.checkMaxActivityLimit(group.getActivities().size());
    String groupId = groupService.createGroup(group);

    if (CollectionUtils.isNotEmpty(memberList)) {
      logger.info("Adding members to the group: {} started", groupId);
      boolean isUseridRedisEnabled =
          Boolean.parseBoolean(
              PropertiesCache.getInstance().getConfigValue(JsonKey.ENABLE_USERID_REDIS_CACHE));
      if (isUseridRedisEnabled) {
        deleteUserCache(memberList);
      }
      Response addMembersRes =
          memberService.handleMemberAddition(
              memberList, groupId, requestHandler.getRequestedBy(actorMessage));
      logger.info(
          "Adding members to the group : {} ended , response {}",
          groupId,
          addMembersRes.getResult());
    }

    Response response = new Response();
    response.put(JsonKey.GROUP_ID, groupId);
    logger.info("group created successfully with groupId {}", groupId);
    sender().tell(response, self());
    String source =
        actorMessage.getContext().get(JsonKey.REQUEST_SOURCE) != null
            ? (String) actorMessage.getContext().get(JsonKey.REQUEST_SOURCE)
            : "";

    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    if (StringUtils.isNotBlank(source)) {
      TelemetryUtil.generateCorrelatedObject(
          source, StringUtils.capitalize(JsonKey.REQUEST_SOURCE), null, correlatedObject);
    }
    Map<String, Object> targetObject = null;
    targetObject =
        TelemetryUtil.generateTargetObject(
            (String) actorMessage.getContext().get(JsonKey.USER_ID),
            TelemetryEnvKey.USER,
            JsonKey.CREATE,
            null);

    TelemetryUtil.generateCorrelatedObject(
        (String) actorMessage.getContext().get(JsonKey.USER_ID),
        TelemetryEnvKey.USER,
        null,
        correlatedObject);
    TelemetryUtil.generateCorrelatedObject(groupId, TelemetryEnvKey.GROUP, null, correlatedObject);
    TelemetryUtil.telemetryProcessingCall(
        actorMessage.getRequest(), targetObject, correlatedObject, actorMessage.getContext());
  }

  public void deleteUserCache(List<Map<String, Object>> memberList) {
    CacheUtil cacheUtil = new CacheUtil();
    logger.info("Delete user cache from redis");
    memberList.forEach(member -> cacheUtil.delCache((String) (member.get(JsonKey.USER_ID))));
  }
}
