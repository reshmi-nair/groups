package org.sunbird.actors;

import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import com.datastax.driver.core.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.CassandraUtil;
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
  Localizer.class
})
@PowerMockIgnore({"javax.management.*"})
public class CreateGroupActorTest extends BaseActorTest {

  private final Props props = Props.create(CreateGroupActor.class);

  private static Request reqObj;

  @BeforeClass
  public static void setUp() throws Exception {
    reqObj = createGroupReq();
    PowerMockito.mockStatic(Localizer.class);
    when(Localizer.getInstance()).thenReturn(null);

    CassandraMocker.setUpEmbeddedCassandra();

    // mock CassandraOperation
    CassandraOperationImpl cassandraOperation = CassandraMocker.mockCassandraOperation();

    // when inserting record to cassandra insert record in to EmbeddedCassandra
    when(cassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(CassandraMocker.getCreateGroupResponse(reqObj));

    when(cassandraOperation.batchInsert(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyList()))
        .thenReturn(CassandraMocker.addMembersToGroup(reqObj));

    List<Map<String, Object>> members =
        (List<Map<String, Object>>) reqObj.getRequest().get(JsonKey.MEMBERS);

    when(cassandraOperation.updateAddSetRecord(
            Mockito.anyString(),
            Mockito.anyString(),
            Mockito.anyMap(),
            Mockito.anyString(),
            Mockito.anyObject()))
        .thenReturn(
            CassandraMocker.updateUserGroup(members, (String) reqObj.getRequest().get(JsonKey.ID)));

    // check record is inserted by querying EmbeddedCassandra
    ResultSet resultSet = EmbeddedCassandra.session.execute(EmbeddedCassandra.selectStatement);
    Response response = CassandraUtil.createResponse(resultSet);
    List<Map<String, Object>> groupList =
        (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
    Assert.assertEquals(reqObj.get(JsonKey.GROUP_NAME), groupList.get(0).get(JsonKey.GROUP_NAME));

    // check members are inserted successfully or not
    ResultSet groupMember =
        EmbeddedCassandra.session.execute(
            EmbeddedCassandra.selectGroupMemberStatement.bind(
                members.get(0).get(JsonKey.USER_ID),
                members.get(0).get(JsonKey.ROLE),
                reqObj.getRequest().get(JsonKey.ID)));
    Response memberRes = CassandraUtil.createResponse(groupMember);
    List<Map<String, Object>> memberList =
        (List<Map<String, Object>>) memberRes.getResult().get(JsonKey.RESPONSE);
    Assert.assertEquals(
        members.get(0).get(JsonKey.USER_ID), memberList.get(0).get(JsonKey.USER_ID));
  }

  @Test
  public void testCreateGroup() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(Duration.ofSeconds(10), Response.class);
    System.out.println(res.getResult());
    Assert.assertTrue(null != res && res.getResponseCode() == 200);
    Assert.assertNotNull(res.getResult().get(JsonKey.GROUP_ID));
  }

  private static Request createGroupReq() {
    Request reqObj = new Request();
    reqObj.setHeaders(headerMap);
    reqObj.setOperation(ActorOperations.CREATE_GROUP.getValue());
    reqObj.getRequest().put(JsonKey.GROUP_NAME, "TestGroup Name");
    reqObj.getRequest().put(JsonKey.GROUP_DESC, "TestGroup Description");
    List<Map<String, Object>> members = new ArrayList<>();
    Map<String, Object> member = new HashMap<>();
    member.put(JsonKey.USER_ID, "userID");
    member.put(JsonKey.STATUS, "active");
    member.put(JsonKey.ROLE, "member");
    members.add(member);
    List<Map<String, Object>> activities = new ArrayList<>();
    Map<String, Object> activity = new HashMap<>();
    activity.put(JsonKey.TYPE, "COURSE");
    activity.put(JsonKey.ID, "courseId");
    activities.add(activity);
    reqObj.getRequest().put(JsonKey.ACTIVITIES, activities);
    reqObj.getRequest().put(JsonKey.MEMBERS, members);
    reqObj.getRequest().put(JsonKey.ID, UUID.randomUUID().toString());
    return reqObj;
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EmbeddedCassandra.close();
  }
}
