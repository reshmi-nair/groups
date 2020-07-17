package org.sunbird.service;

import java.util.List;
import java.util.Map;
import org.sunbird.exception.BaseException;
import org.sunbird.models.Group;
import org.sunbird.models.GroupResponse;
import org.sunbird.models.MemberResponse;
import org.sunbird.response.Response;

public interface GroupService {

  String createGroup(Group groupObj) throws BaseException;

  GroupResponse readGroup(String groupId) throws Exception;

  List<MemberResponse> readGroupMembers(String groupId) throws Exception;

  void readGroupActivities(Map<String, Object> dbResGroup);

  List<GroupResponse> searchGroup(Map<String, Object> searchFilter) throws BaseException;

  Response updateGroup(Group groupObj) throws BaseException;

  List<Map<String, Object>> handleActivityOperations(
      String groupId, Map<String, Object> activityOperationMap) throws BaseException;
}
