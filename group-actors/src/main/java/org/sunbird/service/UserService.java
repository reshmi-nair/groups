package org.sunbird.service;

import java.util.List;
import org.sunbird.exception.BaseException;
import org.sunbird.response.Response;

public interface UserService {

  public Response searchUserByIds(List<String> userIds) throws BaseException;
}
