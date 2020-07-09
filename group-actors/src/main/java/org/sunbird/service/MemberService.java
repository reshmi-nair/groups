package org.sunbird.service;

import java.util.List;
import java.util.Map;
import org.sunbird.exception.BaseException;
import org.sunbird.models.Member;
import org.sunbird.models.MemberResponse;
import org.sunbird.response.Response;

public interface MemberService {

  Response addMembers(List<Member> member) throws BaseException;

  Response editMembers(List<Member> member) throws BaseException;

  Response removeMembers(List<Member> member) throws BaseException;

  public void handleMemberOperations(Map memberOperationMap, String groupId, String contextUserId)
      throws BaseException;

  Response handleMemberAddition(
      List<Map<String, Object>> memberList, String groupId, String contextUserId)
      throws BaseException;

  List<MemberResponse> fetchMembersByGroupIds(List<String> groupIds, List<String> fields)
      throws BaseException;

  Map<String, String> fetchGroupRoleByUser(List<String> groupIds, String userId)
      throws BaseException;
}
