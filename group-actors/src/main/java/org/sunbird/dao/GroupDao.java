package org.sunbird.dao;

import java.util.List;
import java.util.UUID;
import org.sunbird.exception.BaseException;
import org.sunbird.models.Group;
import org.sunbird.response.Response;

/** This interface will have all methods required for group service api. */
public interface GroupDao {

  /**
   * This method will create group and return groupId as success response or throw
   * ProjectCommonException.
   *
   * @param groupObj Group Details.
   * @return group ID.
   */
  String createGroup(Group groupObj) throws BaseException;

  /**
   * This method will read group based on Id and return response Object as success response or throw
   * ProjectCommonException.
   *
   * @param groupId
   * @return responseObj with Group Details.
   */
  Response readGroup(String groupId) throws BaseException;

  /**
   * This method will return group uuids based on userId and return response Object as success
   * response or throw ProjectCommonException.
   *
   * @param userId
   * @return responseObj with Group Details.
   */
  Response readGroupUuidsByUserId(String userId) throws BaseException;

  /**
   * This method will read group based on Id and return response Object as success response or throw
   * ProjectCommonException.
   *
   * @param groupIds
   * @return responseObj with Group Details.
   */
  Response readGroups(List<UUID> groupIds) throws BaseException;

  /**
   * TODO:To be removed Later
   *
   * <p>This method will read group based on Id and return response Object as success response or
   * throw ProjectCommonException.
   *
   * @param
   * @return responseObj with Group Details.
   */
  Response readAllGroups() throws BaseException;
}
