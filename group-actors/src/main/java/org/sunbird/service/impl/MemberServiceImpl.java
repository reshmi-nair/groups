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
    Response response = memberDao.addMembers(member);
    return response;
  }

  @Override
  public Response removeMembers(List<String> memberId, String groupId) throws BaseException {
    return null;
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
    member.setStatus((String) data.get(JsonKey.STATUS));
    member.setUserId((String) data.get(JsonKey.USER_ID));
    member.setCreatedOn(new Timestamp(System.currentTimeMillis()));
    return member;
  }
}
