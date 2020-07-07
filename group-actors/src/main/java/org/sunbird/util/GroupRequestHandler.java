package org.sunbird.util;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.models.Group;
import org.sunbird.request.Request;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.UUID;

public class GroupRequestHandler {

    public Group handleCreateGroupRequest(Request actorMessage) {
        Group group = new Group();
        group.setName((String) actorMessage.getRequest().get(JsonKey.GROUP_NAME));
        group.setDescription((String) actorMessage.getRequest().get(JsonKey.GROUP_DESC));
        String membershipType = (String) actorMessage.getRequest().get(JsonKey.GROUP_MEMBERSHIP_TYPE);
        if(StringUtils.isNotEmpty(membershipType)) {
            group.setMembershipType(membershipType);
        }else{
            group.setMembershipType(JsonKey.INVITE_ONLY);
        }
        group.setStatus(JsonKey.ACTIVE);
        group.setCreatedBy("");// TODO - add from request context
        group.setId(UUID.randomUUID().toString());
        return group;
    }

    public Group handleUpdateGroupRequest(Request actorMessage) {
        Group group = new Group();
        group.setId((String)actorMessage.getRequest().get(JsonKey.GROUP_ID));
        group.setName((String) actorMessage.getRequest().get(JsonKey.GROUP_NAME));
        group.setDescription((String) actorMessage.getRequest().get(JsonKey.GROUP_DESC));
        group.setMembershipType((String) actorMessage.getRequest().get(JsonKey.GROUP_MEMBERSHIP_TYPE));
        String status = (String) actorMessage.getRequest().get(JsonKey.GROUP_STATUS);
        if(StringUtils.isNotEmpty(status) && status.equalsIgnoreCase("INACTIVE")){
            group.setStatus(status);
        }
        group.setUpdatedBy("");// TODO - add from request context
        return group;
    }

}
