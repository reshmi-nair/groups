package org.sunbird.actors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.sunbird.actor.core.ActorConfig;
import org.sunbird.exception.BaseException;
import org.sunbird.models.Group;
import org.sunbird.request.Request;
import org.sunbird.response.Response;
import org.sunbird.service.GroupService;
import org.sunbird.service.GroupServiceImpl;
import org.sunbird.service.MemberService;
import org.sunbird.service.MemberServiceImpl;
import org.sunbird.util.GroupRequestHandler;
import org.sunbird.util.JsonKey;

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
    logger.info("CreateGroup method call");
    GroupService groupService = new GroupServiceImpl();
    MemberService memberService = new MemberServiceImpl();

    GroupRequestHandler requestHandler = new GroupRequestHandler();
    Group group = requestHandler.handleCreateGroupRequest(actorMessage);
    String groupId = groupService.createGroup(group);

    // add creator of group to memberList as admin
    List<Map<String, Object>> memberList = new ArrayList<>();
    Map<String, Object> createdUser = new HashMap<>();
    createdUser.put(JsonKey.USER_ID, requestHandler.getRequestedBy(actorMessage));
    createdUser.put(JsonKey.ROLE, JsonKey.ADMIN);
    memberList.add(createdUser);

    // adding members to group, if members are provided in request
    List<Map<String, Object>> reqMemberList =
        (List<Map<String, Object>>) actorMessage.getRequest().get(JsonKey.MEMBERS);
    logger.info("Adding members to the group: {} started", group.getName());
    if (CollectionUtils.isNotEmpty(reqMemberList)) {
      memberList.addAll(reqMemberList);
    }
    Response addMembersRes =
        memberService.handleMemberAddition(
            memberList, groupId, requestHandler.getRequestedBy(actorMessage));
    logger.info("Adding members to the group ended : {}", addMembersRes.getResult());

    Response response = new Response();
    response.put(JsonKey.GROUP_ID, groupId);
    sender().tell(response, self());
  }
}
