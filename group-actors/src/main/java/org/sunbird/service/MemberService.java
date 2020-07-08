package org.sunbird.service;

import java.util.List;
import java.util.Map;
import org.sunbird.exception.BaseException;
import org.sunbird.models.Member;
import org.sunbird.models.MemberResponse;
import org.sunbird.response.Response;

public interface MemberService {

  Response addMembers(List<Member> member) throws BaseException;

  Response removeMembers(List<String> memberId, String groupId) throws BaseException;

  Response handleMemberAddition(List<Map<String, Object>> memberList, String groupId)
      throws BaseException;

  List<MemberResponse> fetchMembersByGroupIds(List<String> groupIds, List<String> fields)
      throws BaseException;
}
