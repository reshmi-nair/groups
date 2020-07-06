package org.sunbird.actors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.actor.core.ActorConfig;
import org.sunbird.exception.BaseException;
import org.sunbird.message.ResponseCode;
import org.sunbird.request.Request;
import org.sunbird.response.Response;
import org.sunbird.service.GroupService;
import org.sunbird.service.impl.GroupServiceImpl;
import org.sunbird.util.JsonKey;

@ActorConfig(
  tasks = {"searchGroup"},
  asyncTasks = {}
)
public class SearchGroupActor extends BaseActor {
  private GroupService groupService = GroupServiceImpl.getInstance();

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
    logger.info("SearchGroup method call");
    Map<String, Object> searchQueryMap = request.getRequest();
    Map<String, Object> filterMap = (Map<String, Object>) searchQueryMap.get(JsonKey.FILTERS);
    List<Map<String, Object>> groupDetails = groupService.searchGroup(filterMap);
    Map<String, Object> result = new HashMap<>();
    result.put(JsonKey.GROUP, groupDetails);
    Response response = new Response(result, ResponseCode.OK.getCode());
    sender().tell(response, self());
  }
}
