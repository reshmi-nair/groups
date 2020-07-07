package org.sunbird.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.dao.MemberDao;
import org.sunbird.dao.impl.MemberDaoImpl;
import org.sunbird.exception.BaseException;
import org.sunbird.models.Member;
import org.sunbird.response.Response;
import org.sunbird.service.MemberService;
import org.sunbird.util.JsonKey;

public class MemberServiceImpl implements MemberService {

  private static MemberDao memberDao = MemberDaoImpl.getInstance();
  private static MemberService memberService = null;
  private static Logger logger = LoggerFactory.getLogger(MemberServiceImpl.class);
  private static ObjectMapper objectMapper = new ObjectMapper();

  public static MemberService getInstance() {
    if (memberService == null) {
      memberService = new MemberServiceImpl();
    }
    return memberService;
  }

  @Override
  public Response addMembers(List<Member> member) throws BaseException {
    member.forEach(m -> m.setStatus(JsonKey.ACTIVE));
    member.forEach(m -> m.setCreatedBy(""));//TODO - take from request
    member.forEach(m -> m.setCreatedOn(new Timestamp(System.currentTimeMillis())));
    Response response = memberDao.addMembers(member);
    return response;
  }

  @Override
  public Response editMembers(List<Member> member) throws BaseException {
    member.forEach(m -> m.setUpdatedBy(""));//TODO - take from request
    member.forEach(m -> m.setUpdatedOn(new Timestamp(System.currentTimeMillis())));
    Response response = memberDao.editMembers(member);
    return response;
  }

  @Override
  public Response removeMembers(List<Member> member, String groupId) throws BaseException {
    member.forEach(m -> m.setStatus(JsonKey.INACTIVE));
    member.forEach(m -> m.setRemovedBy(""));//TODO - take from request
    member.forEach(m -> m.setRemovedOn(new Timestamp(System.currentTimeMillis())));
    Response response = memberDao.editMembers(member);
    return response;
  }

  public void handleMemberOperations(Map memberOperationMap, String groupId) throws BaseException{
    if (memberOperationMap!=null && !memberOperationMap.isEmpty()) {
      List<Map<String, Object>> memberAddList = (List<Map<String, Object>>)memberOperationMap.get(JsonKey.MEMBER_ADD);
      List<Map<String, Object>> memberEditList = (List<Map<String, Object>>)memberOperationMap.get(JsonKey.MEMBER_EDIT);
      List<String> memberRemoveList = (List<String>)memberOperationMap.get(JsonKey.MEMBER_REMOVE);
      if(memberAddList!=null && !memberAddList.isEmpty()) {
        List<Member> addMembers =
                memberAddList
                        .stream()
                        .map(data -> getMemberModel(data, groupId))
                        .collect(Collectors.toList());
        if (!addMembers.isEmpty()) {
          Response addMemberRes = addMembers(addMembers);
        }
      }
      if(memberEditList!=null && !memberEditList.isEmpty()) {
        List<Member> editMembers =
                memberEditList
                        .stream()
                        .map(data -> getMemberModel(data, groupId))
                        .collect(Collectors.toList());
        if (!editMembers.isEmpty()) {
          Response addMemberRes = editMembers(editMembers);
        }
      }
      if(memberRemoveList!=null && !memberRemoveList.isEmpty()) {
        List<Member> removeMembers =
                memberRemoveList
                        .stream()
                        .map(data -> getMemberModelForRemove(data, groupId))
                        .collect(Collectors.toList());
        if (!removeMembers.isEmpty()) {
          Response addMemberRes = removeMembers(removeMembers, groupId);
        }
      }
    }
  }

  @Override
  public Response handleMemberAddition(List<Map<String, Object>> memberList, String groupId)
      throws BaseException {
    logger.info("Number of members to be added are: {}", memberList.size());
    Response addMemberRes = new Response();
    List<Member> members =
        memberList.stream().map(data -> getMemberModel(data, groupId)).collect(Collectors.toList());
    if (!members.isEmpty()) {
      addMemberRes = addMembers(members);
    }
    return addMemberRes;
  }

  @Override
  public List<Member> fetchMembersByGroupIds(List<String> groupIds, List<String> fields)
      throws BaseException {
    Response response = memberDao.fetchMembersByGroupIds(groupIds, fields);
    List<Member> members = new ArrayList<>();
    if (null != response && null != response.getResult()) {
      List<Map<String, Object>> dbResMembers =
          (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
      if (null != dbResMembers) {
        dbResMembers.forEach(
            map -> {
              Member member = objectMapper.convertValue(map, Member.class);
              members.add(member);
            });
      }
    }

    return members;
  }

  private Member getMemberModel(Map<String, Object> data, String groupId) {
    Member member = new Member();
    member.setGroupId(groupId);
    member.setRole((String) data.get(JsonKey.ROLE));
    member.setUserId((String) data.get(JsonKey.USER_ID));
    return member;
  }

  private Member getMemberModelForRemove(String userId, String groupId) {
    Member member = new Member();
    member.setUserId(userId);
    member.setGroupId(groupId);
    return member;
  }
}
