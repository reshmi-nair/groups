package org.sunbird.actors;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.time.Duration;
import java.util.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.exception.BaseException;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.message.IResponseMessage;
import org.sunbird.message.Localizer;
import org.sunbird.models.ActorOperations;
import org.sunbird.request.Request;
import org.sunbird.response.Response;
import org.sunbird.util.JsonKey;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Localizer.class, ServiceFactory.class})
@PowerMockIgnore({"javax.management.*"})
public class SearchGroupActorTest extends BaseActorTest {
  private static final String GROUP_MEMBER_TABLE = "group_member";
  private final Props props = Props.create(org.sunbird.actors.SearchGroupActor.class);
  public CassandraOperation cassandraOperation;

  @Before
  public void setUp() throws Exception {
    PowerMockito.mockStatic(Localizer.class);
    when(Localizer.getInstance()).thenReturn(null);
    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
  }

  @Test
  public void searchByUserIdFiltersReturnSuccessResponse() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setHeaders(headerMap);
    reqObj.setOperation(ActorOperations.SEARCH_GROUP.getValue());
    Map<String, Object> filters = new HashMap<>();
    filters.put(JsonKey.USER_ID, "userid1");
    reqObj.getRequest().put(JsonKey.FILTERS, filters);
    try {
      when(cassandraOperation.getRecordsByProperty(
              Mockito.anyString(),
              Mockito.anyString(),
              Mockito.anyString(),
              Matchers.eq("userid1")))
          .thenReturn(getGroupSetByUserId());

      when(cassandraOperation.getRecordsByProperties(
              Mockito.anyString(), Matchers.eq(GROUP_MEMBER_TABLE), Mockito.anyMap()))
          .thenReturn(getMemberResponseByGroupIds());

      when(cassandraOperation.getRecordsByProperties(
              Mockito.anyString(), Matchers.eq("group"), Mockito.anyMap()))
          .thenReturn(getGroupsDetailsResponse());

    } catch (BaseException be) {
      Assert.assertTrue(false);
    }
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(Duration.ofSeconds(10), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == 200);
  }

  @Test
  public void searchByEmptyFiltersThrowsBaseException() {
    TestKit probe = new TestKit(system);

    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setHeaders(headerMap);
    reqObj.setOperation(ActorOperations.SEARCH_GROUP.getValue());
    Map<String, Object> filters = new HashMap<>();

    reqObj.getRequest().put(JsonKey.FILTERS, filters);
    subject.tell(reqObj, probe.getRef());

    BaseException ex = probe.expectMsgClass(Duration.ofSeconds(10), BaseException.class);
    Assert.assertEquals(IResponseMessage.MISSING_MANDATORY_PARAMS, ex.getMessage());
  }

  private Response getGroupsDetailsResponse() {
    Map<String, Object> result = new HashMap<>();
    List<Map<String, Object>> groupList = new ArrayList<>();
    Map<String, Object> group1 = new HashMap<>();
    group1.put("name", "TestGroup1");
    group1.put("id", "groupid1");

    Map<String, Object> group2 = new HashMap<>();
    group2.put("name", "TestGroup2");
    group2.put("id", "groupid2");
    groupList.add(group1);
    groupList.add(group2);
    result.put(JsonKey.RESPONSE, groupList);
    Response response = new Response();
    response.putAll(result);
    return response;
  }

  private Response getGroupSetByUserId() {
    Map<String, Object> result = new HashMap<>();
    List<Map<String, Object>> userGroupList = new ArrayList<>();
    Map<String, Object> userGroup = new HashMap<>();
    userGroup.put(JsonKey.USER_ID, "userid1");
    Set<String> groupIdSet = new LinkedHashSet<>();
    groupIdSet.add("groupid1");
    groupIdSet.add("groupid2");
    userGroup.put(JsonKey.GROUP_ID, groupIdSet);
    userGroupList.add(userGroup);
    result.put(JsonKey.RESPONSE, userGroupList);
    Response response = new Response();
    response.putAll(result);
    return response;
  }

  private Response getDBEmptyResponse() {
    Map<String, Object> result = new HashMap<>();
    result.put(JsonKey.RESPONSE, new ArrayList<>());
    Response response = new Response();
    response.putAll(result);
    return response;
  }

  private Response getMemberResponseByGroupIds() {
    Map<String, Object> result = new HashMap<>();
    List<Map<String, Object>> memberLists = new ArrayList<>();
    Map<String, Object> member1 = new HashMap<>();
    member1.put(JsonKey.USER_ID, "userid1");
    member1.put(JsonKey.GROUP_ID, "groupid1");
    member1.put(JsonKey.ROLE, "admin");
    member1.put(JsonKey.STATUS, JsonKey.ACTIVE);
    Map<String, Object> member2 = new HashMap<>();
    member2.put(JsonKey.USER_ID, "userid1");
    member2.put(JsonKey.GROUP_ID, "groupid2");
    member2.put(JsonKey.ROLE, "member");
    member2.put(JsonKey.STATUS, JsonKey.ACTIVE);

    memberLists.add(member1);
    memberLists.add(member2);

    result.put(JsonKey.RESPONSE, memberLists);
    Response response = new Response();
    response.putAll(result);
    return response;
  }
}
