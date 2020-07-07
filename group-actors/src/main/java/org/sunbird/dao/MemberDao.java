package org.sunbird.dao;

import java.util.List;
import org.sunbird.exception.BaseException;
import org.sunbird.models.Member;
import org.sunbird.response.Response;

public interface MemberDao {

  Response addMembers(List<Member> member) throws BaseException;

  Response removeMembers(List<String> members, String groupId) throws BaseException;

  Response fetchMembersByGroupIds(List<String> groupIds, List<String> fields) throws BaseException;
}
