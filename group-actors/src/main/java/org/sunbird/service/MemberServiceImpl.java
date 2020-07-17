package org.sunbird.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.dao.MemberDao;
import org.sunbird.dao.MemberDaoImpl;
import org.sunbird.exception.BaseException;
import org.sunbird.models.Member;
import org.sunbird.models.MemberResponse;
import org.sunbird.response.Response;
import org.sunbird.util.GroupUtil;
import org.sunbird.util.JsonKey;

public class MemberServiceImpl implements MemberService {

  private static MemberDao memberDao = MemberDaoImpl.getInstance();
  private static MemberService memberService = null;
  private static Logger logger = LoggerFactory.getLogger(MemberServiceImpl.class);
  private static ObjectMapper objectMapper = new ObjectMapper();
  private static UserService userService = UserServiceImpl.getInstance();

  @Override
  public Response addMembers(List<Member> member) throws BaseException {
    Response response = memberDao.addMembers(member);
    return response;
  }

  @Override
  public Response editMembers(List<Member> member) throws BaseException {
    Response response = memberDao.editMembers(member);
    return response;
  }

  @Override
  public Response removeMembers(List<Member> member) throws BaseException {
    Response response = memberDao.editMembers(member);
    if (response != null && response.getResult().get(JsonKey.RESPONSE) != null) {
      memberDao.removeMemberFromUserGroup(member);
    }
    return response;
  }

  public void handleMemberOperations(Map memberOperationMap, String groupId, String contextUserId)
      throws BaseException {
    List<Map<String, Object>> memberAddList =
        (List<Map<String, Object>>) memberOperationMap.get(JsonKey.ADD);
    if (CollectionUtils.isNotEmpty(memberAddList)) {
      Response addMemberRes = handleMemberAddition(memberAddList, groupId, contextUserId);
    }
    List<Map<String, Object>> memberEditList =
        (List<Map<String, Object>>) memberOperationMap.get(JsonKey.EDIT);
    if (CollectionUtils.isNotEmpty(memberEditList)) {
      List<Member> editMembers =
          memberEditList
              .stream()
              .map(data -> getMemberModelForEdit(data, groupId, contextUserId))
              .collect(Collectors.toList());
      if (!editMembers.isEmpty()) {
        Response editMemberRes = editMembers(editMembers);
      }
    }
    List<String> memberRemoveList = (List<String>) memberOperationMap.get(JsonKey.REMOVE);
    if (CollectionUtils.isNotEmpty(memberRemoveList)) {
      List<Member> removeMembers =
          memberRemoveList
              .stream()
              .map(data -> getMemberModelForRemove(data, groupId, contextUserId))
              .collect(Collectors.toList());
      if (!removeMembers.isEmpty()) {
        Response removeMemberRes = removeMembers(removeMembers);
      }
    }
  }

  @Override
  public Response handleMemberAddition(
      List<Map<String, Object>> memberList, String groupId, String contextUserId)
      throws BaseException {
    logger.info("Number of members to be added are: {}", memberList.size());
    Response addMemberRes = new Response();
    List<Member> members =
        memberList
            .stream()
            .map(data -> getMemberModelForAdd(data, groupId, contextUserId))
            .collect(Collectors.toList());
    if (!members.isEmpty()) {
      addMemberRes = addMembers(members);
    }
    return addMemberRes;
  }

  // TODO: Fix me to get the Members Details with List<Member> includes name of the user

  /**
   * Fetch Member Details based on Group
   *
   * @param groupIds
   * @param fields
   * @return
   * @throws BaseException
   */
  @Override
  public List<MemberResponse> fetchMembersByGroupIds(List<String> groupIds, List<String> fields)
      throws BaseException {
    Response response = memberDao.fetchMembersByGroupIds(groupIds, fields);
    List<MemberResponse> members = new ArrayList<>();
    if (null != response && null != response.getResult()) {
      List<Map<String, Object>> dbResMembers =
          (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
      if (null != dbResMembers) {
        dbResMembers.forEach(
            map -> {
              Member member = objectMapper.convertValue(map, Member.class);
              if (JsonKey.ACTIVE.equals(member.getStatus())) {
                MemberResponse memberResponse = createMemberResponseObj(member);
                members.add(memberResponse);
              }
            });
      }
    }
    if (!members.isEmpty()) {
      fetchMemberDetails(members);
    }
    return members;
  }

  private void fetchMemberDetails(List<MemberResponse> members) throws BaseException {
    List<String> memberIds = new ArrayList<>();
    members.forEach(
        member -> {
          memberIds.add(member.getUserId());
        });
    Response response = userService.searchUserByIds(memberIds);
    if (null != response && null != response.getResult()) {
      Map<String, Object> memberRes =
          (Map<String, Object>) response.getResult().get(JsonKey.RESPONSE);
      if (null != memberRes) {
        List<Map<String, Object>> userDetails =
            (List<Map<String, Object>>) memberRes.get(JsonKey.CONTENT);
        members.forEach(
            member -> {
              Map<String, Object> userInfo =
                  userDetails
                      .stream()
                      .filter(x -> member.getUserId().equals((String) x.get(JsonKey.ID)))
                      .findFirst()
                      .orElse(null);
              if (userInfo != null) {
                String firstName =
                    StringUtils.isNotEmpty((String) userInfo.get(JsonKey.FIRSTNAME))
                        ? (String) userInfo.get(JsonKey.FIRSTNAME)
                        : "";

                String lastName =
                    StringUtils.isNotEmpty((String) userInfo.get(JsonKey.LASTNAME))
                        ? " " + (String) userInfo.get(JsonKey.LASTNAME)
                        : "";
                member.setName(firstName + lastName);
              }
            });
      }
    }
  }

  @Override
  public Map<String, String> fetchGroupRoleByUser(List<String> groupIds, String userId)
      throws BaseException {
    Response response = memberDao.fetchGroupRoleByUser(groupIds, userId);
    Map<String, String> groupRoleMap = new HashMap<>();
    if (null != response && null != response.getResult()) {
      List<Map<String, Object>> dbResMembers =
          (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
      if (null != dbResMembers) {
        dbResMembers.forEach(
            map -> {
              groupRoleMap.put((String) map.get(JsonKey.GROUP_ID), (String) map.get(JsonKey.ROLE));
            });
      }
    }
    return groupRoleMap;
  }

  private Member getMemberModelForAdd(
      Map<String, Object> data, String groupId, String contextUserId) {
    Member member = new Member();
    member.setGroupId(groupId);
    String role = (String) data.get(JsonKey.ROLE);
    if (StringUtils.isNotEmpty(role)) {
      member.setRole(role);
    } else {
      member.setRole(JsonKey.MEMBER);
    }
    member.setUserId((String) data.get(JsonKey.USER_ID));
    member.setStatus(JsonKey.ACTIVE);
    member.setCreatedBy(contextUserId);
    member.setCreatedOn(new Timestamp(System.currentTimeMillis()));

    return member;
  }

  private Member getMemberModelForEdit(
      Map<String, Object> data, String groupId, String contextUserId) {
    Member member = new Member();
    member.setGroupId(groupId);
    String role = (String) data.get(JsonKey.ROLE);
    if (StringUtils.isNotEmpty(role)) {
      member.setRole(role);
    } else {
      member.setRole(JsonKey.MEMBER);
    }
    member.setUserId((String) data.get(JsonKey.USER_ID));
    member.setUpdatedBy(contextUserId);
    member.setUpdatedOn(new Timestamp(System.currentTimeMillis()));

    return member;
  }

  private Member getMemberModelForRemove(String userId, String groupId, String contextUserId) {
    Member member = new Member();
    member.setGroupId(groupId);
    member.setUserId(userId);
    member.setStatus(JsonKey.INACTIVE);
    member.setRemovedBy(contextUserId);
    member.setRemovedOn(new Timestamp(System.currentTimeMillis()));

    return member;
  }

  private MemberResponse createMemberResponseObj(Member member) {
    MemberResponse memberResponse = new MemberResponse();
    memberResponse.setGroupId(member.getGroupId());
    memberResponse.setUserId(member.getUserId());
    memberResponse.setRole(member.getRole());
    memberResponse.setStatus(member.getStatus());
    memberResponse.setCreatedBy(member.getCreatedBy());
    memberResponse.setRemovedBy(member.getRemovedBy());
    memberResponse.setUpdatedBy(member.getUpdatedBy());
    memberResponse.setCreatedOn(
        member.getCreatedOn() != null
            ? GroupUtil.convertTimestampToUTC(member.getCreatedOn().getTime())
            : null);
    memberResponse.setUpdatedOn(
        member.getUpdatedOn() != null
            ? GroupUtil.convertTimestampToUTC(member.getUpdatedOn().getTime())
            : null);
    memberResponse.setRemovedOn(
        member.getRemovedOn() != null
            ? GroupUtil.convertTimestampToUTC(member.getRemovedOn().getTime())
            : null);
    return memberResponse;
  }
}
