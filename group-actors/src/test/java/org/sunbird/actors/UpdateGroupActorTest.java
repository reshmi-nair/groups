package org.sunbird.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.exception.BaseException;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.message.Localizer;
import org.sunbird.models.ActorOperations;
import org.sunbird.request.Request;
import org.sunbird.response.Response;
import org.sunbird.util.JsonKey;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  CassandraOperation.class,
  CassandraOperationImpl.class,
  ServiceFactory.class,
  Localizer.class
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
  }

  @Test
  public void testUpdateGroup() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    try {
      when(cassandraOperation.updateRecord(
              Mockito.anyString(), Mockito.anyString(), Mockito.anyObject()))
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
    Map<String,List<Map<String, Object>>> memberOpearations = new HashMap<>();

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
    memberOpearations.put("add",members);
    //Edit
    members = new ArrayList<>();
    member = new HashMap<>();
    member.put(JsonKey.USER_ID, "userID1");
    member.put(JsonKey.ROLE, "admin");
    members.add(member);
    memberOpearations.put("edit",members);
    //Remove
    ArrayList removeMembers = new ArrayList();
    removeMembers.add("userID1");
    memberOpearations.put("remove",removeMembers);
    reqObj.getRequest().put(JsonKey.MEMBERS, memberOpearations);
    reqObj.getRequest().put(JsonKey.ID, "group1");
    return reqObj;
  }

}
