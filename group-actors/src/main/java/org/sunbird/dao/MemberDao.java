package org.sunbird.dao;

import java.util.List;
import java.util.Map;
import org.sunbird.exception.BaseException;
import org.sunbird.models.Member;
import org.sunbird.response.Response;

public interface MemberDao {

  Response addMembers(List<Member> member) throws BaseException;

  Response editMembers(List<Member> member) throws BaseException;

  Response fetchMembersByGroupIds(List<String> groupIds) throws BaseException;

  Response fetchGroupRoleByUser(List<String> groupIds, String userId) throws BaseException;

  /**
   * This method will return group uuids based on userId and return response Object as success
   * response or throw ProjectCommonException.
   *
   * @param userId
   * @return responseObj with Group Details.
   */
  Response readGroupIdsByUserId(String userId) throws BaseException;

  Response readGroupIdsByUserIds(List<String> memberList) throws BaseException;

  Response upsertGroupInUserGroup(Map<String, Object> userGroupMap) throws BaseException;

  Response updateGroupInUserGroup(Map<String, Object> userGroupMap, String userId) throws BaseException;

  void deleteFromUserGroup(String userId) throws BaseException;

}
