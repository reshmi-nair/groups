package controllers;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.junit.Before;
import org.junit.Test;
import org.sunbird.exception.BaseException;
import play.mvc.Result;

public class ReadGroupControllerTest extends BaseApplicationTest {

  @Before
  public void before() throws BaseException {
    setup(DummyActor.class);
  }

  @Test
  public void testReadGroupPasses() {
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put("groupId", "group1");
    Map<String, Object> request = new HashMap<>();
    request.put("request", reqMap);
    Result result = performTest("/v1/group/read/:group1", "GET", request);
    assertTrue(getResponseStatus(result) == Response.Status.OK.getStatusCode());
  }
}
