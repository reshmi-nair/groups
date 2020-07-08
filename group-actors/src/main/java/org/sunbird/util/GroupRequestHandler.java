package org.sunbird.util;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.models.Group;
import org.sunbird.request.Request;

public class GroupRequestHandler {

  Logger logger = LoggerFactory.getLogger(GroupRequestHandler.class);

  public Group handleCreateGroupRequest(Request actorMessage) {
    Group group = new Group();
    group.setName((String) actorMessage.getRequest().get(JsonKey.GROUP_NAME));
    group.setDescription((String) actorMessage.getRequest().get(JsonKey.GROUP_DESC));
    String membershipType = (String) actorMessage.getRequest().get(JsonKey.GROUP_MEMBERSHIP_TYPE);
    if (StringUtils.isNotEmpty(membershipType)) {
      group.setMembershipType(membershipType);
    } else {
      group.setMembershipType(JsonKey.INVITE_ONLY);
    }
    group.setStatus(JsonKey.ACTIVE);
    group.setCreatedBy(""); // TODO - add from request context
    group.setId(UUID.randomUUID().toString());
    List<Map<String, Object>> activityList =
        (List<Map<String, Object>>) actorMessage.getRequest().get(JsonKey.ACTIVITIES);
    if (CollectionUtils.isNotEmpty(activityList)) {
      logger.info("adding activities to the group {}", activityList.size());
      group.setActivities(new HashSet<>(activityList));
    }
    return group;
  }

  public Group handleUpdateGroupRequest(Request actorMessage) {
    Group group = new Group();
    group.setId((String) actorMessage.getRequest().get(JsonKey.GROUP_ID));
    group.setName((String) actorMessage.getRequest().get(JsonKey.GROUP_NAME));
    group.setDescription((String) actorMessage.getRequest().get(JsonKey.GROUP_DESC));
    group.setMembershipType((String) actorMessage.getRequest().get(JsonKey.GROUP_MEMBERSHIP_TYPE));
    String status = (String) actorMessage.getRequest().get(JsonKey.GROUP_STATUS);
    if (StringUtils.isNotEmpty(status) && status.equalsIgnoreCase(JsonKey.INACTIVE)) {
      group.setStatus(status);
    }
    group.setUpdatedBy(""); // TODO - add from request context
    return group;
  }
}
