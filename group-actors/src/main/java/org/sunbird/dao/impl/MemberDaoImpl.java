package org.sunbird.dao.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.dao.MemberDao;
import org.sunbird.exception.BaseException;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.models.Member;
import org.sunbird.response.Response;
import org.sunbird.util.DBUtil;
import org.sunbird.util.JsonKey;

public class MemberDaoImpl implements MemberDao {

  private static final String GROUP_MEMBER_TABLE = "group_member";
  private static final String USER_GROUP_TABLE = "user_group";
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private ObjectMapper mapper = new ObjectMapper();
  private static MemberDao memberDao = null;
  private Logger logger = LoggerFactory.getLogger(MemberDaoImpl.class);

  public static MemberDao getInstance() {
    if (memberDao == null) {
      memberDao = new MemberDaoImpl();
    }
    return memberDao;
  }

  @Override
  public Response addMembers(List<Member> member) throws BaseException {
    List<Map<String, Object>> memberList =
        mapper.convertValue(member, new TypeReference<List<Map<String, Object>>>() {});
    Response response =
        cassandraOperation.batchInsert(DBUtil.KEY_SPACE_NAME, GROUP_MEMBER_TABLE, memberList);
    updateUserGroupTable(memberList);
    return response;
  }

  private Response updateUserGroupTable(List<Map<String, Object>> memberList) throws BaseException {
    logger.info("User Group table updation started ");
    Response response = null;
    for (Map<String, Object> member : memberList) {
      Map<String, Object> primaryKey = new HashMap<>();
      primaryKey.put(JsonKey.USER_ID, member.get(JsonKey.USER_ID));
      response =
          cassandraOperation.updateAddSetRecord(
              DBUtil.KEY_SPACE_NAME,
              USER_GROUP_TABLE,
              primaryKey,
              JsonKey.GROUP_ID,
              member.get(JsonKey.GROUP_ID));
    }
    return response;
  }

  @Override
  public Response removeMembers(List<String> members, String groupId) throws BaseException {
    return null;
  }

  @Override
  public Response fetchMembersByGroupIds(List<String> groupIds, List<String> fields)
      throws BaseException {
    Map<String, Object> properties = new HashMap<>();
    properties.put(JsonKey.GROUP_ID, groupIds);
    properties.put(JsonKey.STATUS, JsonKey.ACTIVE);

    Response responseObj =
        cassandraOperation.getRecordsByProperties(
            DBUtil.KEY_SPACE_NAME, GROUP_MEMBER_TABLE, properties, fields);
    return responseObj;
  }
}
