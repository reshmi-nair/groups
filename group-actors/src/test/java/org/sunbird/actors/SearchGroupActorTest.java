package org.sunbird.actors;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.exception.BaseException;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.message.Localizer;
import org.sunbird.models.ActorOperations;
import org.sunbird.request.Request;
import org.sunbird.response.Response;
import org.sunbird.util.JsonKey;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Localizer.class, ServiceFactory.class})
@PowerMockIgnore({"javax.management.*"})
public class SearchGroupActorTest extends BaseActorTest {

  private final Props props = Props.create(org.sunbird.actors.SearchGroupActor.class);
  public static CassandraOperation cassandraOperation;

  @Before
  public void setUp() throws Exception {
    PowerMockito.mockStatic(Localizer.class);
    when(Localizer.getInstance()).thenReturn(null);

    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
  }

  @Test
  public void searchByEmptyFiltersReturnSuccessResponse() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setHeaders(headerMap);
    reqObj.setOperation(ActorOperations.SEARCH_GROUP.getValue());
    Map<String, Object> filters = new HashMap<>();

    reqObj.getRequest().put(JsonKey.FILTERS, filters);
    try {
      when(cassandraOperation.getAllRecords(Mockito.anyString(), Mockito.anyString()))
          .thenReturn(getSearchResultResponse());
    } catch (BaseException be) {
      Assert.assertTrue(false);
    }
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(Duration.ofSeconds(10), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == 200);
  }

  @Test
  public void searchByEmptyFiltersReturnAllGroups() {
    TestKit probe = new TestKit(system);

    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setHeaders(headerMap);
    reqObj.setOperation(ActorOperations.SEARCH_GROUP.getValue());
    Map<String, Object> filters = new HashMap<>();

    reqObj.getRequest().put(JsonKey.FILTERS, filters);
    try {
      when(cassandraOperation.getAllRecords(Mockito.anyString(), Mockito.anyString()))
          .thenReturn(getSearchResultResponse());
    } catch (BaseException be) {
      Assert.assertTrue(false);
    }
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(Duration.ofSeconds(10), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == 200);
    Map<String, Object> resultMap = res.getResult();
    List<Map<String, Object>> groups = (List<Map<String, Object>>) resultMap.get(JsonKey.GROUP);
    Assert.assertEquals(2, groups.size());
    Assert.assertEquals("TestGroup1", groups.get(0).get("name"));
  }

  @Test
  public void searchByEmptyFiltersThrowBaseException() {
    TestKit probe = new TestKit(system);

    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setHeaders(headerMap);
    reqObj.setOperation(ActorOperations.SEARCH_GROUP.getValue());
    Map<String, Object> filters = new HashMap<>();

    reqObj.getRequest().put(JsonKey.FILTERS, filters);
    try {
      when(cassandraOperation.getAllRecords(Mockito.anyString(), Mockito.anyString()))
          .thenThrow(BaseException.class);
    } catch (BaseException be) {
      Assert.assertTrue(true);
    }
    subject.tell(reqObj, probe.getRef());
  }

  private Response getSearchResultResponse() {
    Map<String, Object> result = new HashMap<>();
    List<Map<String, Object>> groupList = new ArrayList<>();
    Map<String, Object> group1 = new HashMap<>();
    group1.put("name", "TestGroup1");
    group1.put("id", "id1");

    Map<String, Object> group2 = new HashMap<>();
    group2.put("name", "TestGroup2");
    group2.put("id", "id2");
    groupList.add(group1);
    groupList.add(group2);
    result.put(JsonKey.RESPONSE, groupList);
    Response response = new Response();
    response.putAll(result);
    return response;
  }
}
