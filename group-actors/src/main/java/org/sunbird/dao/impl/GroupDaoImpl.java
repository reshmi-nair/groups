package org.sunbird.dao.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.dao.GroupDao;
import org.sunbird.exception.BaseException;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.models.Group;
import org.sunbird.response.Response;
import org.sunbird.util.DBUtil;
import org.sunbird.util.JsonKey;

public class GroupDaoImpl implements GroupDao {
  private static final String GROUP_TABLE_NAME = "group";
  private static final String USER_GROUP_TABLE_NAME = "user_group";
  private static final String GROUP_MEMBER_TABLE_NAME = "group_member";

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private ObjectMapper mapper = new ObjectMapper();
  private static GroupDao groupDao = null;

  public static GroupDao getInstance() {
    if (groupDao == null) {
      groupDao = new GroupDaoImpl();
    }
    return groupDao;
  }

  @Override
  public String createGroup(Group groupObj) throws BaseException {
    Map<String, Object> map = mapper.convertValue(groupObj, Map.class);
    cassandraOperation.insertRecord(DBUtil.KEY_SPACE_NAME, GROUP_TABLE_NAME, map);
    return (String) map.get(JsonKey.ID);
  }

  @Override
  public Response readGroup(String groupId) throws BaseException {
    Response responseObj =
        cassandraOperation.getRecordById(DBUtil.KEY_SPACE_NAME, GROUP_TABLE_NAME, groupId);
    return responseObj;
  }

  @Override
  public Response readGroupUuidsByUserId(String userId) throws BaseException {
    UUID usrId = java.util.UUID.fromString(userId);
    Response responseObj =
        cassandraOperation.getRecordsByProperty(
            DBUtil.KEY_SPACE_NAME, USER_GROUP_TABLE_NAME, JsonKey.USER_ID, usrId);
    return responseObj;
  }

  @Override
  public Response readGroups(List<UUID> groupIds) throws BaseException {
    Response responseObj =
        cassandraOperation.getRecordsByProperty(
            DBUtil.KEY_SPACE_NAME, GROUP_TABLE_NAME, JsonKey.ID, new ArrayList<Object>(groupIds));
    return responseObj;
  }

  @Override
  public Response readAllGroups() throws BaseException {
    Response responseObj =
        cassandraOperation.getAllRecords(DBUtil.KEY_SPACE_NAME, GROUP_TABLE_NAME);
    return responseObj;
  }
}
