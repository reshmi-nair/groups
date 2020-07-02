package controllers;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.junit.Before;
import org.junit.Test;
import play.mvc.Result;

public class HealthControllerTest extends BaseApplicationTest {

  @Before
  public void setUp() throws Exception {
    setup(DummyActor.class);
  }

  @Test
  public void testGetHealthPasses() {
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put("accept", "yes");
    Result result = performTest("/health", "GET", reqMap);
    assertTrue(getResponseStatus(result) == Response.Status.OK.getStatusCode());
  }

  @Test
  public void testGetServiceHealthPasses() {
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put("accept", "yes");
    Result result = performTest("/service/health", "GET", reqMap);
    assertTrue(getResponseStatus(result) == Response.Status.OK.getStatusCode());
  }

  @Test
  public void testPostHealthFails() {
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put("accept", "yes");
    Result result = performTest("/health", "POST", reqMap);
    assertTrue(getResponseStatus(result) == Response.Status.NOT_FOUND.getStatusCode());
  }
}
