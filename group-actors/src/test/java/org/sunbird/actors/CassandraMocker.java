package org.sunbird.actors;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.powermock.api.mockito.PowerMockito;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.Constants;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.request.Request;
import org.sunbird.response.Response;
import org.sunbird.util.JsonKey;

public class CassandraMocker {

  public static void setUpEmbeddedCassandra() throws Exception {
    EmbeddedCassandra.setUp();
    EmbeddedCassandra.createStatements();
  }

  public static CassandraOperationImpl mockCassandraOperation() {
    CassandraOperationImpl cassandraOperation;
    // mock cassandra
    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    return cassandraOperation;
  }

  public static Response getCreateGroupResponse(Request request) {
    Assert.assertNotNull(EmbeddedCassandra.session.getCluster().getClusterName());
    ResultSet resultSet =
        EmbeddedCassandra.session.execute(
            QueryBuilder.select()
                .all()
                .from(EmbeddedCassandra.KEYSPACE, EmbeddedCassandra.GROUP_TABLE));
    // initially group table is empty
    Assert.assertEquals(0, resultSet.all().size());
    EmbeddedCassandra.session.execute(
        EmbeddedCassandra.insertStatement.bind(
            request.getRequest().get(JsonKey.ID),
            request.getRequest().get(JsonKey.GROUP_NAME),
            request.getRequest().get(JsonKey.GROUP_DESC)));
    Response response = new Response();
    response.put(Constants.RESPONSE, Constants.SUCCESS);
    return response;
  }

  // add members to group-member table
  public static Response addMembersToGroup(Request request) {
    List<Map<String, Object>> memberList =
        (List<Map<String, Object>>) request.getRequest().get(JsonKey.MEMBERS);
    for (Map<String, Object> member : memberList) {
      EmbeddedCassandra.session.execute(
          EmbeddedCassandra.insertMemberStatement.bind(
              request.getRequest().get(JsonKey.ID),
              member.get(JsonKey.USER_ID),
              member.get(JsonKey.ROLE),
              member.get(JsonKey.STATUS)));
    }
    Response response = new Response();
    response.put(Constants.RESPONSE, Constants.SUCCESS);
    return response;
  }

  // update user_group table
  public static Response updateUserGroup(List<Map<String, Object>> members, String groupId) {
    for (Map<String, Object> member : members) {
      EmbeddedCassandra.session.execute(
          EmbeddedCassandra.updateUserGroupStatement
              .bind()
              .setSet(JsonKey.GROUP_ID, ImmutableSet.of(groupId))
              .setString(JsonKey.USER_ID, (String) member.get(JsonKey.USER_ID)));
    }

    Response response = new Response();
    response.put(Constants.RESPONSE, Constants.SUCCESS);
    return response;
  }
}
