package org.sunbird.actors;

import org.sunbird.actor.core.ActorConfig;
import org.sunbird.exception.BaseException;
import org.sunbird.message.ResponseCode;
import org.sunbird.request.Request;
import org.sunbird.response.Response;
import org.sunbird.service.GroupService;
import org.sunbird.service.impl.GroupServiceImpl;
import org.sunbird.util.JsonKey;

@ActorConfig(
  tasks = {"readGroup"},
  asyncTasks = {}
)
public class ReadGroupActor extends BaseActor {

  private GroupService groupService = GroupServiceImpl.getInstance();

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();
    switch (operation) {
      case "readGroup":
        readGroup(request);
        break;
      default:
        onReceiveUnsupportedMessage("ReadGroupActor");
    }
  }
  /**
   * This method will read group in cassandra based on group id.
   *
   * @param actorMessage
   */
  private void readGroup(Request actorMessage) throws BaseException {
    logger.info("ReadGroup method call");
    String groupId = (String) actorMessage.getRequest().get(JsonKey.GROUP_ID);

    Response response = groupService.readGroup(groupId);
    response.setResponseCode(ResponseCode.OK.getCode());
    sender().tell(response, self());
  }
}
