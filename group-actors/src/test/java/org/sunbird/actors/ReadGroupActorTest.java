package org.sunbird.actors;

import static org.powermock.api.mockito.PowerMockito.*;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.Application;
import org.sunbird.cache.impl.RedisCache;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.exception.BaseException;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.message.Localizer;
import org.sunbird.models.ActivitySearchRequestConfig;
import org.sunbird.models.ActorOperations;
import org.sunbird.models.SearchRequest;
import org.sunbird.request.Request;
import org.sunbird.response.Response;
import org.sunbird.util.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  Localizer.class,
  ServiceFactory.class,
  HttpClientUtil.class,
  ActivityConfigReader.class,
  Application.class,
  RedisCache.class
})
@PowerMockIgnore({"javax.management.*"})
public class ReadGroupActorTest extends BaseActorTest {
  private static final String GROUP_MEMBER_TABLE = "group_member";
  public static String ACTIVITY_CONFIG_JSON = "activityConfigTest.json";

  private final Props props = Props.create(ReadGroupActor.class);
  public CassandraOperation cassandraOperation;
  private ObjectMapper mapper = new ObjectMapper();

  @Before
  public void beforeEachTest() throws Exception {
    PowerMockito.mockStatic(Localizer.class);
    when(Localizer.getInstance()).thenReturn(null);

    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    mockCacheActor();
  }

  private void mockCacheActor() throws Exception {
    ActorSystem actorSystem = ActorSystem.create("system");
    Props props = Props.create(CacheActor.class);
    ActorRef actorRef = actorSystem.actorOf(props);
    Application app = PowerMockito.mock(Application.class);
    PowerMockito.mockStatic(Application.class);
    PowerMockito.when(Application.getInstance()).thenReturn(app);
    PowerMockito.when(app.getActorRef(Mockito.anyString())).thenReturn(actorRef);
    app.init();
    PowerMockito.mockStatic(RedisCache.class);
    PowerMockito.doNothing()
        .when(RedisCache.class, "set", Mockito.anyString(), Mockito.anyString(), Mockito.anyInt());
    PowerMockito.when(RedisCache.get(Mockito.anyString(), Mockito.anyObject(), Mockito.anyInt()))
        .thenReturn("");
  }

  @Test
  public void readGroupWithMembers() throws Exception {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.READ_GROUP.getValue());
    reqObj.getRequest().put(JsonKey.GROUP_ID, "TestGroup");
    reqObj.getRequest().put(JsonKey.FIELDS, Arrays.asList("members"));
    try {
      when(cassandraOperation.getRecordById(
              Mockito.anyString(), Mockito.anyString(), Matchers.eq("TestGroup")))
          .thenReturn(getGroupsDetailsResponseNoActivities());
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

  @Ignore
  @Test
  public void readGroupReturnGroupWithActivites() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.READ_GROUP.getValue());
    reqObj.getRequest().put(JsonKey.GROUP_ID, "groupid1");
    reqObj.getRequest().put(JsonKey.FIELDS, Arrays.asList("activities"));

    try {
      when(cassandraOperation.getRecordById(
              Mockito.anyString(), Mockito.anyString(), Matchers.eq("groupid1")))
          .thenReturn(getGroupsDetailsResponse());
      PowerMockito.mockStatic(HttpClientUtil.class);
      when(HttpClientUtil.post(Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
          .thenReturn(getActivityInfoResponse());
      PowerMockito.mockStatic(ActivityConfigReader.class);
      when(ActivityConfigReader.getServiceUtilClassName(Mockito.anyString()))
          .thenReturn(new ContentSearchUtil());
      when(ActivityConfigReader.getFieldsLists(Mockito.any(SearchServiceUtil.class)))
          .thenReturn(new ArrayList<>());

    } catch (BaseException ex) {
      Assert.assertTrue(false);
    }
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(Duration.ofSeconds(10), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == 200);
    List<Map<String, Object>> activities =
        (List<Map<String, Object>>) res.getResult().get(JsonKey.ACTIVITIES);
    Map<String, Object> activityInfo =
        (Map<String, Object>) activities.get(0).get(JsonKey.ACTIVITY_INFO);
    Assert.assertTrue(
        null != activityInfo && !activityInfo.isEmpty() && null != activityInfo.get("identifier"));
  }

  @Test
  public void readGroupWithMembersActivities() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.READ_GROUP.getValue());
    reqObj.getRequest().put(JsonKey.GROUP_ID, "TestGroup");
    reqObj.getRequest().put(JsonKey.FIELDS, Arrays.asList("members", "activities"));
    try {
      when(cassandraOperation.getRecordById(
              Mockito.anyString(), Mockito.anyString(), Matchers.eq("TestGroup")))
          .thenReturn(getGroupsDetailsResponseNoActivities());
      when(cassandraOperation.getRecordsByProperties(
              Mockito.anyString(),
              Matchers.eq(GROUP_MEMBER_TABLE),
              Mockito.anyMap(),
              Mockito.anyList()))
          .thenReturn(getMemberResponseByGroupIds());
      PowerMockito.mockStatic(HttpClientUtil.class);
      when(HttpClientUtil.post(Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
          .thenReturn(getUserServiceResponse());
      when(HttpClientUtil.post(Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
          .thenReturn(getActivityInfoResponse());
      PowerMockito.mockStatic(ActivityConfigReader.class);
      when(ActivityConfigReader.getServiceUtilClassName(Mockito.anyString()))
          .thenReturn(new ContentSearchUtil());
      when(ActivityConfigReader.getFieldsLists(Mockito.any(SearchServiceUtil.class)))
          .thenReturn(new ArrayList<>());

    } catch (BaseException | JsonProcessingException be) {
      Assert.assertTrue(false);
    }
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(Duration.ofSeconds(10), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == 200);
  }

  private String getActivityInfoResponse() {
    String response =
        "{ \"id\": \"api.content.search\", \"ver\": \"1.0\", \"ts\": \"2020-07-10T12:36:10.076Z\", \"params\": { \"resmsgid\": "
            + "\"eeda31c0-c2a9-11ea-8ebb-8389e3bbdc82\", \"msgid\": \"eed88410-c2a9-11ea-8ebb-8389e3bbdc82\", \"status\": \"successful\", "
            + "\"err\": null, \"errmsg\": null }, \"responseCode\": \"OK\", \"result\": { \"count\": 2, "
            + "\"content\": [ { \"identifier\": \"do_11304065892935270414\", \"contentType\": \"TextBook\", \"objectType\": \"Content\" },"
            + " { \"identifier\": \"do_112470675618004992181\", \"contentType\": \"Course\", \"objectType\": \"Content\" } ] } }";
    return response;
  }

  private String getEmptyActivityResponse() {
    String response =
        "{ \"id\": \"api.content.search\", \"ver\": \"1.0\", \"ts\": \"2020-07-10T12:43:47.609Z\", \"params\": "
            + "{ \"resmsgid\": \"ff903090-c2aa-11ea-8ebb-8389e3bbdc82\", \"msgid\": \"ff8e34c0-c2aa-11ea-8ebb-8389e3bbdc82\", \"status\": \"successful\", \"err\": null, \"errmsg\": null },"
            + " \"responseCode\": \"OK\", \"result\": { \"count\": 0 } }";
    return response;
  }

  private Response getGroupsDetailsResponse() {
    Map<String, Object> result = new HashMap<>();
    List<Map<String, Object>> groupList = new ArrayList<>();
    Map<String, Object> group1 = new HashMap<>();
    group1.put("name", "TestGroup1");
    group1.put("id", "groupid1");
    group1.put("status", "active");
    List<Map<String, Object>> activities = new ArrayList<>();
    Map<String, Object> activity1 = new HashMap<>();
    activity1.put(JsonKey.ID, "do_112470675618004992181");
    activity1.put(JsonKey.TYPE, "Course");

    Map<String, Object> activity2 = new HashMap<>();
    activity2.put(JsonKey.ID, "do_11304065892935270414");
    activity2.put(JsonKey.TYPE, "Textbook");
    activities.add(activity1);
    activities.add(activity2);
    group1.put(JsonKey.ACTIVITIES, activities);
    groupList.add(group1);
    result.put(JsonKey.RESPONSE, groupList);
    Response response = new Response();
    response.putAll(result);
    return response;
  }

  private Response getGroupsDetailsResponseNoActivities() {
    Map<String, Object> result = new HashMap<>();
    List<Map<String, Object>> groupList = new ArrayList<>();
    Map<String, Object> group1 = new HashMap<>();
    group1.put("name", "TestGroup1");
    group1.put("id", "groupid1");
    group1.put("status", "active");
    groupList.add(group1);
    result.put(JsonKey.RESPONSE, groupList);
    Response response = new Response();
    response.putAll(result);
    return response;
  }

  private List<ActivitySearchRequestConfig> getActivitySearchConfigList() {
    List<ActivitySearchRequestConfig> configLists = new ArrayList<>();
    ActivitySearchRequestConfig activitySearchRequestConfig = new ActivitySearchRequestConfig();
    activitySearchRequestConfig.setApiUrl("http://content-service/v1/user/search");
    activitySearchRequestConfig.setIdentifierKey("identifier");
    activitySearchRequestConfig.setRequestHeader(new HashMap<>());
    activitySearchRequestConfig.setSearchRequest(new SearchRequest());
    activitySearchRequestConfig.setResponse("$.result.content[*]");
    configLists.add(activitySearchRequestConfig);
    return configLists;
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
    member1.put(JsonKey.ID, "user7");
    member1.put(JsonKey.FIRSTNAME, "John");
    member1.put(JsonKey.LASTNAME, null);
    Map<String, Object> member2 = new HashMap<>();
    member2.put(JsonKey.ID, "user6");
    member2.put(JsonKey.FIRSTNAME, "Terry");
    member2.put(JsonKey.LASTNAME, "Test");
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
