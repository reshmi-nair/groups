package org.sunbird.dao;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.CassandraUtil;
import org.sunbird.common.Constants;
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
    return response;
  }

  @Override
  public Response readGroupIdsByUserId(String userId) throws BaseException {
    List<String> userIds = new ArrayList<>();
    userIds.add(userId);
    return readGroupIdsByUserIds(userIds);
  }

  public Response readGroupIdsByUserIds(List<String> memberList) throws BaseException {
    Response responseObj =
            cassandraOperation.getRecordsByPrimaryKeys(
                    DBUtil.KEY_SPACE_NAME, USER_GROUP_TABLE, memberList,JsonKey.USER_ID);
    return responseObj;
  }

  public Response upsertGroupInUserGroup(Map<String, Object> userGroupMap) throws BaseException {
    Response responseObj =
            cassandraOperation.upsertRecord(DBUtil.KEY_SPACE_NAME, USER_GROUP_TABLE, userGroupMap);
    return responseObj;
  }

  public Response updateGroupInUserGroup(Map<String, Object> userGroupMap, String userId) throws BaseException {
    Map<String, Object> compositeKeyMap = new HashMap<>();
    compositeKeyMap.put(JsonKey.USER_ID, userId);

    Response responseObj =
            cassandraOperation.updateRecord(DBUtil.KEY_SPACE_NAME, USER_GROUP_TABLE, userGroupMap, compositeKeyMap);
    return responseObj;
  }

  public void deleteFromUserGroup(String userId) throws BaseException {
    Map<String, String> compositeKeyMap = new HashMap<>();
    compositeKeyMap.put(JsonKey.USER_ID, userId);
    cassandraOperation.deleteRecord(DBUtil.KEY_SPACE_NAME, USER_GROUP_TABLE,compositeKeyMap);
  }

  @Override
  public Response editMembers(List<Member> member) throws BaseException {
    List<Map<String, Map<String, Object>>> list = new ArrayList<>();
    for (Member memberObj : member) {
      list.add(CassandraUtil.batchUpdateQuery(memberObj));
    }
    for (Map<String, Map<String, Object>> record : list) {
      Map<String, Object> nonPKRecord = record.get(Constants.NON_PRIMARY_KEY);
      Map<String, Object> filteredNonPKRecord =
          nonPKRecord
              .entrySet()
              .stream()
              .filter(map -> (map.getValue() != null))
              .collect(Collectors.toMap(map -> map.getKey(), map -> map.getValue()));
      record.put(Constants.NON_PRIMARY_KEY, filteredNonPKRecord);
    }
    Response response =
        cassandraOperation.batchUpdate(DBUtil.KEY_SPACE_NAME, GROUP_MEMBER_TABLE, list);
    return response;
  }

  @Override
  public Response fetchMembersByGroupIds(List<String> groupIds)
      throws BaseException {
    Map<String, Object> properties = new HashMap<>();
    properties.put(JsonKey.GROUP_ID, groupIds);
    Response responseObj =
        cassandraOperation.getRecordsByPrimaryKeys(
            DBUtil.KEY_SPACE_NAME, GROUP_MEMBER_TABLE, groupIds, JsonKey.GROUP_ID);
    return responseObj;
  }

  @Override
  public Response fetchGroupRoleByUser(List<String> groupIds, String userId) throws BaseException {
    Map<String, Object> properties = new LinkedHashMap<>();
    properties.put(JsonKey.GROUP_ID, groupIds);
    properties.put(JsonKey.USER_ID, userId);
    Response responseObj =
        cassandraOperation.getRecordsByProperties(
            DBUtil.KEY_SPACE_NAME, GROUP_MEMBER_TABLE, properties);
    return responseObj;
  }

}
