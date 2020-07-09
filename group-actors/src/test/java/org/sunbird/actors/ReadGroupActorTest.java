package org.sunbird.actors;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.sunbird.message.Localizer;
import org.sunbird.models.ActorOperations;
import org.sunbird.request.Request;
import org.sunbird.response.Response;
import org.sunbird.util.HttpClientUtil;
import org.sunbird.util.JsonKey;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Localizer.class, ServiceFactory.class, HttpClientUtil.class})
@PowerMockIgnore({"javax.management.*"})
public class ReadGroupActorTest extends BaseActorTest {
  private static final String GROUP_MEMBER_TABLE = "group_member";

  private final Props props = Props.create(ReadGroupActor.class);
  public CassandraOperation cassandraOperation;
  private ObjectMapper mapper = new ObjectMapper();

  @Before
  public void beforeEachTest() {
    PowerMockito.mockStatic(Localizer.class);
    when(Localizer.getInstance()).thenReturn(null);

    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
  }

  @Test
  public void readGroupWithMembers() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.READ_GROUP.getValue());
    reqObj.getRequest().put(JsonKey.GROUP_ID, "TestGroup");
    try {
      when(cassandraOperation.getRecordById(
              Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
          .thenReturn(getGroupsDetailsResponse());
      when(cassandraOperation.getRecordsByProperties(
              Mockito.anyString(),
              Matchers.eq(GROUP_MEMBER_TABLE),
              Mockito.anyMap(),
              Mockito.anyList()))
          .thenReturn(getMemberResponseByGroupIds());
      PowerMockito.mockStatic(HttpClientUtil.class);
      when(HttpClientUtil.post(Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
          .thenReturn(getUserServiceResponse());

    } catch (BaseException | JsonProcessingException be) {
      Assert.assertTrue(false);
    }
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(Duration.ofSeconds(10), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == 200);
  }

  private Response getGroupsDetailsResponse() {
    Map<String, Object> result = new HashMap<>();
    List<Map<String, Object>> groupList = new ArrayList<>();
    Map<String, Object> group1 = new HashMap<>();
    group1.put("name", "TestGroup1");
    group1.put("id", "groupid1");
    groupList.add(group1);
    result.put(JsonKey.RESPONSE, groupList);
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
    member2.put(JsonKey.USER_ID, "userid2");
    member2.put(JsonKey.GROUP_ID, "groupid1");
    member2.put(JsonKey.ROLE, "member");
    member2.put(JsonKey.STATUS, JsonKey.ACTIVE);

    memberLists.add(member1);
    memberLists.add(member2);

    result.put(JsonKey.RESPONSE, memberLists);
    Response response = new Response();
    response.putAll(result);
    return response;
  }

  private String getUserServiceResponse() throws JsonProcessingException {
    Map<String, Object> result = new HashMap<>();
    List<Map<String, Object>> userList = new ArrayList<>();
    Map<String, Object> member1 = new HashMap<>();
    member1.put(JsonKey.ID, "userid1");
    member1.put(JsonKey.USERNAME, "John");
    Map<String, Object> member2 = new HashMap<>();
    member2.put(JsonKey.ID, "userid2");
    member2.put(JsonKey.USERNAME, "Terry");
    userList.add(member1);
    userList.add(member2);
    Map<String, Object> content = new HashMap<>();
    content.put(JsonKey.CONTENT, userList);
    result.put(JsonKey.RESPONSE, content);
    Response response = new Response();
    response.putAll(result);
    String jsonStr = mapper.writeValueAsString(response);

    return jsonStr;
  }
}
