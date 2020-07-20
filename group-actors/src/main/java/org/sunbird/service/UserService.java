package org.sunbird.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.exception.BaseException;
import org.sunbird.response.Response;

public interface UserService {

  public Response searchUserByIds(List<String> userIds) throws BaseException;

  public Response getSystemSettings() throws BaseException;

  public Response getOrganisationDetails(String orgId) throws BaseException;

  default void getUpdatedRequestHeader(Map<String, String> header) {
    if (null == header) {
      header = new HashMap<>();
    }
    header.put("Content-Type", "application/json");
  }
}
