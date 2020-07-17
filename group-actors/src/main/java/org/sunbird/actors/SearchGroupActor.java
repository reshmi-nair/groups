package org.sunbird.actors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.actor.core.ActorConfig;
import org.sunbird.exception.BaseException;
import org.sunbird.message.ResponseCode;
import org.sunbird.models.GroupResponse;
import org.sunbird.request.Request;
import org.sunbird.response.Response;
import org.sunbird.service.GroupService;
import org.sunbird.service.GroupServiceImpl;
import org.sunbird.util.JsonKey;

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
    logger.info("SearchGroup method call");
    GroupService groupService = new GroupServiceImpl();
    Map<String, Object> searchQueryMap = request.getRequest();
    Map<String, Object> filterMap = (Map<String, Object>) searchQueryMap.get(JsonKey.FILTERS);
    List<GroupResponse> groupDetails = groupService.searchGroup(filterMap);
    Map<String, Object> result = new HashMap<>();
    groupDetails.sort(
        ((o1, o2) ->
            o1.getMemberRole().compareTo(o2.getMemberRole()))); // sort group result by role
    result.put(JsonKey.GROUP, groupDetails);
    Response response = new Response(result, ResponseCode.OK.getCode());
    sender().tell(response, self());
  }
}
