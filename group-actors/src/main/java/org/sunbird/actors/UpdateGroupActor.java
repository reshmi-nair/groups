package org.sunbird.actors;

import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.MapUtils;
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
import org.sunbird.util.CacheUtil;
import org.sunbird.util.GroupRequestHandler;
import org.sunbird.util.JsonKey;

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
    GroupService groupService = new GroupServiceImpl();
    MemberService memberService = new MemberServiceImpl();

    GroupRequestHandler requestHandler = new GroupRequestHandler();
    Group group = requestHandler.handleUpdateGroupRequest(actorMessage);

    // member operations to group
    Map memberOperationMap = (Map) actorMessage.getRequest().get(JsonKey.MEMBERS);
    if (MapUtils.isNotEmpty(memberOperationMap)) {
      cacheUtil.delCache(group.getId() + "_" + JsonKey.MEMBERS);
      memberService.handleMemberOperations(
          memberOperationMap, group.getId(), requestHandler.getRequestedBy(actorMessage));
    }

    Map<String, Object> activityOperationMap =
        (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ACTIVITIES);
    if (MapUtils.isNotEmpty(activityOperationMap)) {
      cacheUtil.delCache(group.getId());
      List<Map<String, Object>> updateActivityList =
          groupService.handleActivityOperations(group.getId(), activityOperationMap);
      group.setActivities(updateActivityList);
    }

    Response response = groupService.updateGroup(group);
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    response.setResponseCode(ResponseCode.OK.getCode());
    sender().tell(response, self());
  }
}
