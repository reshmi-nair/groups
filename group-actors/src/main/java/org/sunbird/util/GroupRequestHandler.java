package org.sunbird.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.models.Group;
import org.sunbird.models.MemberResponse;
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
    group.setCreatedBy(getRequestedBy(actorMessage));
    group.setId(UUID.randomUUID().toString());
    List<Map<String, Object>> activityList =
        (List<Map<String, Object>>) actorMessage.getRequest().get(JsonKey.ACTIVITIES);
    if (CollectionUtils.isNotEmpty(activityList)) {
      logger.info("adding activities to the group {} are {}", group.getId(), activityList.size());
      group.setActivities(activityList);
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
    group.setUpdatedBy(getRequestedBy(actorMessage));

    return group;
  }

  public String getRequestedBy(Request actorMessage) {
    String contextUserId = (String) actorMessage.getContext().get(JsonKey.USER_ID);
    String managedFor = (String) actorMessage.getContext().get(JsonKey.MANAGED_FOR);

    logger.info(JsonKey.USER_ID + " in getRequestedBy(): " + contextUserId);
    logger.info(JsonKey.MANAGED_FOR + " in getRequestedBy(): " + managedFor);

    // If MUA, then use that userid for createdby and updateby
    if (StringUtils.isNotEmpty(managedFor)) {
      contextUserId = managedFor;
    }
    return contextUserId;
  }

  public void validateAddMembers(Map memberOperationMap, List<MemberResponse> membersInDB, Map validationErrors) {
    List<Map<String, Object>> memberAddList =
            (List<Map<String, Object>>) memberOperationMap.get(JsonKey.ADD);
    List errorList = (ArrayList)validationErrors.get(JsonKey.MEMBERS);
    List<Map<String, Object>> newMemberAddList = new ArrayList<Map<String, Object>>();
    if (CollectionUtils.isNotEmpty(memberAddList)) {
      // Check if members in add request is already existing, if not create new list
      if (CollectionUtils.isNotEmpty(membersInDB)) {
        memberAddList
                .stream()
                .forEach(
                        e ->
                        {
                          if (membersInDB
                                  .stream()
                                  .filter(d -> d.getUserId().equals(e.get(JsonKey.USER_ID)))
                                  .count() > 0) {
                            Map errorMap = new HashMap<>();
                            errorMap.put(JsonKey.USER_ID, e.get(JsonKey.USER_ID));
                            errorMap.put(JsonKey.ERROR_CODE, JsonKey.MEMBER_EXISTS);
                            errorList.add(errorMap);
                          } else {
                            //Remove duplicates in new list
                            if (!newMemberAddList.stream().anyMatch(mi -> mi.get(JsonKey.USER_ID).equals(e.get(JsonKey.USER_ID)))) {
                              newMemberAddList.add(e);
                            }
                          }
                        });
        if (CollectionUtils.isNotEmpty(newMemberAddList)) {
          memberOperationMap.put(JsonKey.ADD, newMemberAddList);
        }else {
          //If records are in DB and newMemberAddList is empty, means all members to add are already existing
          memberOperationMap.put(JsonKey.ADD, new ArrayList<Map<String, Object>>());
        }
      }
    }
  }
  public void validateEditMembers(Map memberOperationMap, List<MemberResponse> membersInDB, Map validationErrors) {
    List<Map<String, Object>> memberEditList =
            (List<Map<String, Object>>) memberOperationMap.get(JsonKey.EDIT);
    List errorList = (ArrayList)validationErrors.get(JsonKey.MEMBERS);
    List<Map<String, Object>> newMemberEditList = new ArrayList<Map<String, Object>>();
    if (CollectionUtils.isNotEmpty(memberEditList)) {
      // Check if members in edit request is not existing, add errors
      if (CollectionUtils.isNotEmpty(membersInDB)) {
        memberEditList
                .stream()
                .forEach(
                        e ->
                        {
                          if(membersInDB
                                  .stream()
                                  .filter(d -> d.getUserId().equals(e.get(JsonKey.USER_ID)))
                                  .count()<1) {
                            Map errorMap = new HashMap<>();
                            errorMap.put(JsonKey.USER_ID,e.get(JsonKey.USER_ID));
                            errorMap.put(JsonKey.ERROR_CODE, JsonKey.MEMBER_NOT_FOUND);
                            errorList.add(errorMap);
                          }else {
                            //Remove duplicates in new list
                            if (!newMemberEditList.stream().anyMatch(mi -> mi.get(JsonKey.USER_ID).equals(e.get(JsonKey.USER_ID)))) {
                              newMemberEditList.add(e);
                            }
                          }
                        });
        if (CollectionUtils.isNotEmpty(newMemberEditList)) {
          memberOperationMap.put(JsonKey.EDIT, newMemberEditList);
        }else {
          //If records are in DB and newMemberEditList is empty, means all new members are not existing
          memberOperationMap.put(JsonKey.EDIT, new ArrayList<Map<String, Object>>());
        }
      }
    }
  }
  public void validateRemoveMembers(Map memberOperationMap, List<MemberResponse> membersInDB, Map validationErrors) {
    List<String> memberRemoveList = (List<String>) memberOperationMap.get(JsonKey.REMOVE);
    List errorList = (ArrayList) validationErrors.get(JsonKey.MEMBERS);
    List<String> newMemberRemoveList = new ArrayList<String>();
    if (CollectionUtils.isNotEmpty(memberRemoveList)) {
      // Check if members in remove request is not existing, add to validation error
      //If existing, then add to new remove list
      if (CollectionUtils.isNotEmpty(membersInDB)) {
        memberRemoveList
                .stream()
                .forEach(
                        e ->
                        {
                          if(membersInDB
                                  .stream()
                                  .filter(d -> d.getUserId().equals(e))
                                  .count()<1) {
                            Map errorMap = new HashMap<>();
                            errorMap.put(JsonKey.USER_ID,e);
                            errorMap.put(JsonKey.ERROR_CODE, JsonKey.MEMBER_NOT_FOUND);
                            errorList.add(errorMap);
                          }else{
                            //Remove duplicates
                            if (!newMemberRemoveList.stream().anyMatch(mi -> mi.equals(e))) {
                              newMemberRemoveList.add(e);
                            }
                          }
                        });
        if(CollectionUtils.isNotEmpty(newMemberRemoveList)){
          memberOperationMap.put(JsonKey.REMOVE,newMemberRemoveList);
        }else {
          //If records are in DB and newMemberRemoveList is empty, means nothing to remove
          memberOperationMap.put(JsonKey.REMOVE, new ArrayList<Map<String, Object>>());
        }
      }
    }
  }
}
