package controllers;

import org.junit.Before;
import org.junit.Test;
import org.sunbird.exception.BaseException;
import org.sunbird.request.Request;
import org.sunbird.util.JsonKey;
import play.mvc.Result;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class UpdateGroupControllerTest extends BaseApplicationTest {

  @Before
  public void before() throws BaseException {
    setup(DummyActor.class);
  }

  @Test
  public void testUpdateGroupPasses() {
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.GROUP_ID, "group1");
    reqMap.put(JsonKey.GROUP_NAME, "group name1");
    Map<String, Object> request = new HashMap<>();
    List<Map<String, Object>> members = new ArrayList<>();
    Map<String, Object> member = new HashMap<>();
    member.put(JsonKey.ROLE, JsonKey.MEMBER);
    member.put(JsonKey.STATUS, JsonKey.ACTIVE);
    member.put(JsonKey.USER_ID, "userID");
    members.add(member);
    reqMap.put(JsonKey.MEMBERS, members);
    request.put("request", reqMap);

    Result result = performTest("/v1/group/update", "PATCH", request);
    assertTrue(getResponseStatus(result) == Response.Status.OK.getStatusCode());
  }

  @Test
  public void testEmptyGroupId() {
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.GROUP_ID, "");
    Map<String, Object> request = new HashMap<>();
    request.put("request", reqMap);
    Result result = performTest("/v1/group/update", "PATCH", request);
    System.out.println(getResponseStatus(result));
    assertTrue(getResponseStatus(result) == Response.Status.BAD_REQUEST.getStatusCode());
  }

  @Test
  public void testMandatoryParamGroupId() {
    Map<String, Object> reqMap = new HashMap<>();
    Map<String, Object> request = new HashMap<>();
    request.put("request", reqMap);
    Result result = performTest("/v1/group/update", "PATCH", request);
    assertTrue(getResponseStatus(result) == Response.Status.BAD_REQUEST.getStatusCode());
  }

}
