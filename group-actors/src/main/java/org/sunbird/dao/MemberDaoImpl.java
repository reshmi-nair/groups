package org.sunbird.dao;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
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
    addGroupInUserGroup(memberList);
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

  private void addGroupInUserGroup(List<Map<String, Object>> memberList) throws BaseException {
    logger.info(
            "User Group table update started for the group id {}",
            memberList.get(0).get(JsonKey.GROUP_ID));

    List<String> members =
            memberList
                    .stream()
                    .map(data -> (String) data.get(JsonKey.USER_ID))
                    .collect(Collectors.toList());

    Response userGroupResponseObj = readGroupIdsByUserIds(members);
    memberList
            .stream()
            .forEach(data -> {
              Map<String, Object> userGroupMap = new HashMap<>();
              if (null != userGroupResponseObj && null != userGroupResponseObj.getResult()) {
                List<Map<String, Object>> dbResGroupIds = (List<Map<String, Object>>) userGroupResponseObj.getResult().get(JsonKey.RESPONSE);
                if (CollectionUtils.isNotEmpty(dbResGroupIds)) {
                  Map<String, Object> userMap =dbResGroupIds
                      .stream()
                           .filter(dbMap -> ((String) data.get(JsonKey.USER_ID)).equals((String)dbMap.get(JsonKey.USER_ID)))
                           .findFirst().orElse(null);
                  if(MapUtils.isEmpty(userMap)){
                    createUserGroupRecord(new HashSet<>(), data, userGroupMap);
                  }else {
                    createUserGroupRecord((Set<String>) userMap.get(JsonKey.GROUP_ID), data, userGroupMap);
                  }
                }else {
                  createUserGroupRecord(new HashSet<>(), data, userGroupMap);
                }
              }
              if(MapUtils.isNotEmpty(userGroupMap)) {
                cassandraOperation.upsertRecord(DBUtil.KEY_SPACE_NAME, USER_GROUP_TABLE, userGroupMap);
              }
  });
  }

  private void createUserGroupRecord(Set<String> groupSet, Map<String, Object> data, Map<String, Object> userGroupMap) {
    groupSet.add((String) data.get(JsonKey.GROUP_ID));
    userGroupMap.put(JsonKey.USER_ID, (String) data.get(JsonKey.USER_ID));
    userGroupMap.put(JsonKey.GROUP_ID, groupSet);
  }

  public void removeGroupInUserGroup(List<Member> memberList) throws BaseException {
    logger.info(
            "User Group table update started for the group id {}",
            memberList.get(0).getGroupId());
    List<String> members =
            memberList
                    .stream()
                    .map(data -> (String) data.getUserId())
                    .collect(Collectors.toList());

    Response userGroupResponseObj = readGroupIdsByUserIds(members);
    memberList
            .stream()
            .forEach(data -> {
              Map<String, Object> userGroupMap = new HashMap<>();
              if (null != userGroupResponseObj && null != userGroupResponseObj.getResult()) {
                List<Map<String, Object>> dbResGroupIds = (List<Map<String, Object>>) userGroupResponseObj.getResult().get(JsonKey.RESPONSE);
                if (CollectionUtils.isNotEmpty(dbResGroupIds)) {
                  dbResGroupIds
                          .stream()
                          .forEach(map -> {
                            if(((String) data.getUserId()).equals((String)map.get(JsonKey.USER_ID))){
                              Set<String> groupIdsSet = (Set<String>) map.get(JsonKey.GROUP_ID);
                              groupIdsSet.remove(data.getGroupId());
                              if(groupIdsSet.size()==0){
                                Map<String, String> compositeKeyMap = new HashMap<>();
                                compositeKeyMap.put(JsonKey.USER_ID, data.getUserId());
                                cassandraOperation.deleteRecord(DBUtil.KEY_SPACE_NAME, USER_GROUP_TABLE,compositeKeyMap);
                              }else{
                                userGroupMap.put(JsonKey.GROUP_ID, groupIdsSet);
                              }
                            }
                          });
                }
              }
              if(MapUtils.isNotEmpty(userGroupMap)){
                Map<String, Object> compositeKeyMap = new HashMap<>();
                compositeKeyMap.put(JsonKey.USER_ID, data.getUserId());
                cassandraOperation.updateRecord(DBUtil.KEY_SPACE_NAME, USER_GROUP_TABLE, userGroupMap, compositeKeyMap);
              }
            });
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
        cassandraOperation.getRecordsByCompositeKey(
            DBUtil.KEY_SPACE_NAME, GROUP_MEMBER_TABLE, properties);
    return responseObj;
  }

}
