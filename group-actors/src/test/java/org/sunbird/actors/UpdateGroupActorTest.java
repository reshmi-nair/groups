package org.sunbird.actors;

import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.Application;
import org.sunbird.cache.impl.RedisCache;
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
@PrepareForTest({
  CassandraOperation.class,
  CassandraOperationImpl.class,
  ServiceFactory.class,
  Localizer.class,
  Application.class,
  RedisCache.class
})
@PowerMockIgnore({"javax.management.*"})
public class UpdateGroupActorTest extends BaseActorTest {

  private final Props props = Props.create(UpdateGroupActor.class);
  private Logger logger = LoggerFactory.getLogger(UpdateGroupActorTest.class);
  public static CassandraOperation cassandraOperation;

  @Before
  public void setUp() throws Exception {
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
    PowerMockito.mockStatic(RedisCache.class);
    doNothing()
        .when(RedisCache.class, "set", Mockito.anyString(), Mockito.anyString(), Mockito.anyInt());
    doNothing().when(RedisCache.class, "delete", Mockito.anyObject());
    when(RedisCache.get(Mockito.anyString(), Mockito.anyObject(), Mockito.anyInt())).thenReturn("");
  }

  @Test
  public void testUpdateGroup() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    try {
      when(cassandraOperation.updateRecord(
              Mockito.anyString(), Mockito.anyString(), Mockito.anyObject()))
          .thenReturn(getCassandraResponse());
      when(cassandraOperation.batchInsert(
              Mockito.anyString(), Mockito.anyString(), Mockito.anyList()))
          .thenReturn(getCassandraResponse());
      when(cassandraOperation.updateAddSetRecord(
              Mockito.anyString(),
              Mockito.anyString(),
              Mockito.anyMap(),
              Mockito.anyString(),
              Mockito.anyObject()))
          .thenReturn(getCassandraResponse())
          .thenReturn(getCassandraResponse());
      when(cassandraOperation.updateRemoveSetRecord(
              Mockito.anyString(),
              Mockito.anyString(),
              Mockito.anyMap(),
              Mockito.anyString(),
              Mockito.anyObject()))
          .thenReturn(getCassandraResponse());
      when(cassandraOperation.batchUpdate(
              Mockito.anyString(), Mockito.anyString(), Mockito.anyList()))
          .thenReturn(getCassandraResponse());
    } catch (BaseException be) {
      Assert.assertTrue(false);
    }

    Request reqObj = updateGroupReq();
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(Duration.ofSeconds(10), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == 200);
  }

  private static Request updateGroupReq() {
    Request reqObj = new Request();
    reqObj.setHeaders(headerMap);
    reqObj.setOperation(ActorOperations.UPDATE_GROUP.getValue());
    reqObj.getRequest().put(JsonKey.GROUP_NAME, "TestGroup Name1");
    Map<String, List<Map<String, Object>>> memberOpearations = new HashMap<>();
    List<Map<String, Object>> members = new ArrayList<>();
    Map<String, Object> member = new HashMap<>();
    member.put(JsonKey.USER_ID, "userID1");
    member.put(JsonKey.STATUS, "active");
    member.put(JsonKey.ROLE, "member");
    members.add(member);
    member = new HashMap<>();
    member.put(JsonKey.USER_ID, "userID2");
    member.put(JsonKey.STATUS, "active");
    member.put(JsonKey.ROLE, "member");
    members.add(member);
    memberOpearations.put("add", members);
    // Edit
    members = new ArrayList<>();
    member = new HashMap<>();
    member.put(JsonKey.USER_ID, "userID1");
    member.put(JsonKey.ROLE, "admin");
    members.add(member);
    memberOpearations.put("edit", members);
    // Remove
    ArrayList removeMembers = new ArrayList();
    removeMembers.add("userID1");
    memberOpearations.put("remove", removeMembers);
    // Add activities
    Map<String, List<Map<String, Object>>> activitiesOperations = new HashMap<>();
    List<Map<String, Object>> activities = new ArrayList<>();
    Map<String, Object> activity = new HashMap<>();
    activity.put(JsonKey.TYPE, "COURSE");
    activity.put(JsonKey.ID, "activityId1");
    activities.add(activity);
    activity = new HashMap<>();
    activity.put(JsonKey.TYPE, "COURSE");
    activity.put(JsonKey.ID, "activityId2");
    activities.add(activity);
    activitiesOperations.put("add", activities);
    // Remove
    ArrayList removeActivities = new ArrayList();
    removeActivities.add("activityId2");
    activitiesOperations.put("remove", removeActivities);
    reqObj.getRequest().put(JsonKey.ACTIVITIES, activitiesOperations);
    reqObj.getRequest().put(JsonKey.MEMBERS, memberOpearations);
    reqObj.getRequest().put(JsonKey.GROUP_ID, "group1");
    return reqObj;
  }
}
