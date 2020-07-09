package org.sunbird.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.dao.GroupDao;
import org.sunbird.dao.impl.GroupDaoImpl;
import org.sunbird.exception.BaseException;
import org.sunbird.message.IResponseMessage;
import org.sunbird.message.ResponseCode;
import org.sunbird.models.Group;
import org.sunbird.models.GroupResponse;
import org.sunbird.models.MemberResponse;
import org.sunbird.response.Response;
import org.sunbird.service.GroupService;
import org.sunbird.service.MemberService;
import org.sunbird.util.GroupUtil;
import org.sunbird.util.JsonKey;

public class GroupServiceImpl implements GroupService {
  private static Logger logger = LoggerFactory.getLogger(GroupServiceImpl.class);

  private static GroupDao groupDao = GroupDaoImpl.getInstance();
  private static GroupService groupService = null;
  private static MemberService memberService = MemberServiceImpl.getInstance();
  private static ObjectMapper objectMapper = new ObjectMapper();

  public static GroupService getInstance() {
    if (groupService == null) {
      groupService = new GroupServiceImpl();
    }
    return groupService;
  }

  @Override
  public String createGroup(Group groupObj) throws BaseException {
    String groupId = groupDao.createGroup(groupObj);
    return groupId;
  }

  @Override
  public Map<String, Object> readGroup(String groupId) throws BaseException {
    Map<String, Object> dbResGroup = new HashMap<>();
    Response responseObj = groupDao.readGroup(groupId);
    if (null != responseObj && null != responseObj.getResult()) {

      List<Map<String, Object>> dbGroupDetails =
          (List<Map<String, Object>>) responseObj.getResult().get(JsonKey.RESPONSE);
      if (null != dbGroupDetails && !dbGroupDetails.isEmpty()) {

        dbResGroup = dbGroupDetails.get(0);

        // update createdOn, updatedOn format to utc "yyyy-MM-dd HH:mm:ss:SSSZ

        dbResGroup.put(
            JsonKey.CREATED_ON,
            dbResGroup.get(JsonKey.CREATED_ON) != null
                ? GroupUtil.convertDateToUTC((Date) dbResGroup.get(JsonKey.CREATED_ON))
                : dbResGroup.get(JsonKey.CREATED_ON));

        dbResGroup.put(
            JsonKey.UPDATED_ON,
            dbResGroup.get(JsonKey.UPDATED_ON) != null
                ? GroupUtil.convertDateToUTC((Date) dbResGroup.get(JsonKey.UPDATED_ON))
                : dbResGroup.get(JsonKey.UPDATED_ON));

        List<MemberResponse> members =
            memberService.fetchMembersByGroupIds(Lists.newArrayList(groupId), null);
        dbResGroup.put(JsonKey.MEMBERS, members);
      }
    }
    return dbResGroup;
  }

  /**
   * TODO: Extraction of specific fields needs to be added This method will search group details
   * based on filters and return all group details as success response or throw BaseException.
   *
   * @param searchFilter .
   * @return dbGroupDetails.
   */
  @Override
  public List<GroupResponse> searchGroup(Map<String, Object> searchFilter) throws BaseException {
    List<GroupResponse> groups = new ArrayList<>();
    String userId = (String) searchFilter.get(JsonKey.USER_ID);
    String groupId = (String) searchFilter.get(JsonKey.GROUP_ID);
    if (StringUtils.isNotBlank(userId)) {
      List<String> groupIds = fetchAllGroupIdsByUserId(userId);
      if (!groupIds.isEmpty()) {
        Map<String, String> groupRoleMap = memberService.fetchGroupRoleByUser(groupIds, userId);
        groups = readGroupDetailsByGroupIds(groupIds);
        GroupUtil.updateRoles(groups, groupRoleMap);
      }

    } else if (StringUtils.isNotBlank(groupId)) {
      List<String> groupIds = Lists.newArrayList(groupId);
      groups = readGroupDetailsByGroupIds(groupIds);
    } else {
      logger.error("Bad Request userId or GroupId is Mandatory");
      throw new BaseException(
          IResponseMessage.INVALID_REQUESTED_DATA,
          IResponseMessage.MISSING_MANDATORY_PARAMS,
          ResponseCode.BAD_REQUEST.getCode());
    }
    return groups;
  }

  /**
   * Get all groupsIds By userIds
   *
   * @param userId
   * @return groupIdsList
   * @throws BaseException
   */
  private List<String> fetchAllGroupIdsByUserId(String userId) throws BaseException {
    Response groupIdsResponse = groupDao.readGroupIdsByUserId(userId);
    if (null != groupIdsResponse && null != groupIdsResponse.getResult()) {
      List<Map<String, Object>> dbResGroupIds =
          (List<Map<String, Object>>) groupIdsResponse.getResult().get(JsonKey.RESPONSE);
      if (null != dbResGroupIds && !dbResGroupIds.isEmpty()) {
        Set<String> groupIdsSet = (Set<String>) dbResGroupIds.get(0).get(JsonKey.GROUP_ID);
        return new ArrayList<>(groupIdsSet);
      }
    }
    return new ArrayList<>();
  }

  /**
   * Read groups details based on group Ids
   *
   * @param groupIds
   * @return
   * @throws BaseException
   */
  private List<GroupResponse> readGroupDetailsByGroupIds(List<String> groupIds)
      throws BaseException {
    List<GroupResponse> groups = new ArrayList<>();
    Response response = groupDao.readGroups(groupIds);
    if (null != response && null != response.getResult()) {
      List<Map<String, Object>> dbGroupDetails =
          (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
      if (null != dbGroupDetails) {
        dbGroupDetails.forEach(
            map -> {
              Group group = objectMapper.convertValue(map, Group.class);
              GroupResponse groupResponse = createGroupResponseObj(group);
              groups.add(groupResponse);
            });
      }
    }
    return groups;
  }

  @Override
  public Response updateGroup(Group groupObj) throws BaseException {
    Response responseObj = groupDao.updateGroup(groupObj);
    return responseObj;
  }

  private GroupResponse createGroupResponseObj(Group group) {
    GroupResponse groupResponse = new GroupResponse();
    groupResponse.setId(group.getId());
    groupResponse.setDescription(group.getDescription());
    groupResponse.setName(group.getName());
    groupResponse.setStatus(group.getStatus());
    groupResponse.setActivities(group.getActivities());
    groupResponse.setCreatedBy(group.getCreatedBy());
    groupResponse.setUpdatedBy(group.getUpdatedBy());
    groupResponse.setCreatedOn(
        group.getCreatedOn() != null
            ? GroupUtil.convertTimestampToUTC(group.getCreatedOn().getTime())
            : null);
    groupResponse.setUpdatedOn(
        group.getUpdatedOn() != null
            ? GroupUtil.convertTimestampToUTC(group.getUpdatedOn().getTime())
            : null);
    return groupResponse;
  }
}
