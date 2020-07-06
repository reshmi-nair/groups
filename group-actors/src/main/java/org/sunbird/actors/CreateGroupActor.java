package org.sunbird.actors;

import org.sunbird.actor.core.ActorConfig;
import org.sunbird.exception.BaseException;
import org.sunbird.models.Group;
import org.sunbird.request.Request;
import org.sunbird.response.Response;
import org.sunbird.service.GroupService;
import org.sunbird.service.impl.GroupServiceImpl;
import org.sunbird.util.JsonKey;

@ActorConfig(
  tasks = {"createGroup", "updateGroup"},
  asyncTasks = {}
)
public class CreateGroupActor extends BaseActor {

  private GroupService groupService = GroupServiceImpl.getInstance();

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();
    switch (operation) {
      case "createGroup":
        createGroup(request);
        break;
      case "updateGroup":
        // updateGroup(request);
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

    Group group = new Group();
    group.setName((String) actorMessage.getRequest().get(JsonKey.GROUP_NAME));
    group.setDescription((String) actorMessage.getRequest().get(JsonKey.GROUP_DESC));
    String groupId = groupService.createGroup(group);
    Response response = new Response();
    response.put(JsonKey.GROUP_ID, groupId);
    sender().tell(response, self());
  }
}
