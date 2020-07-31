package org.sunbird.dao;

import java.util.List;
import org.sunbird.exception.BaseException;
import org.sunbird.models.Member;
import org.sunbird.response.Response;

public interface MemberDao {

  Response addMembers(List<Member> member) throws BaseException;

  Response editMembers(List<Member> member) throws BaseException;

  Response fetchMembersByGroupIds(List<String> groupIds) throws BaseException;

  Response fetchGroupRoleByUser(List<String> groupIds, String userId) throws BaseException;

  void removeGroupInUserGroup(List<Member> member) throws BaseException;

  /**
   * This method will return group uuids based on userId and return response Object as success
   * response or throw ProjectCommonException.
   *
   * @param userId
   * @return responseObj with Group Details.
   */
  Response readGroupIdsByUserId(String userId) throws BaseException;

}
