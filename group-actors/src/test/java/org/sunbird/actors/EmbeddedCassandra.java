package org.sunbird.actors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.dataset.cql.FileCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.sunbird.util.JsonKey;

public class EmbeddedCassandra {

  static final String KEYSPACE = "sunbird";
  static final String GROUP_TABLE = "group";
  static final String MEMBER_TABLE = "group_member";
  static final String USER_GROUP = "user_group";
  static Session session;
  static BoundStatement insertStatement;
  static BoundStatement selectStatement;
  static BoundStatement insertMemberStatement;
  static BoundStatement updateUserGroupStatement;
  static BoundStatement selectGroupMemberStatement;

  private static CQLDataLoader dataLoader;

  public static void setUp() throws Exception {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(30000L);
    session = EmbeddedCassandraServerHelper.getSession();
    dataLoader = new CQLDataLoader(session);
    dataLoader.load(new ClassPathCQLDataSet("createGroup.cql", KEYSPACE));
  }

  public void loadData(String cqlFile) {
    dataLoader.load(new FileCQLDataSet(cqlFile, false, false));
  }

  public static void createStatements() {

    // create prepared statements
    PreparedStatement insertGroupQuery =
        session.prepare(
            QueryBuilder.insertInto(KEYSPACE, GROUP_TABLE)
                .value(JsonKey.ID, bindMarker())
                .value(JsonKey.GROUP_NAME, bindMarker())
                .value(JsonKey.GROUP_DESC, bindMarker()));

    // link prepared statements to boundstatements
    PreparedStatement selectGroupQuery =
        session.prepare(QueryBuilder.select().all().from(KEYSPACE, GROUP_TABLE));

    PreparedStatement insertMemberQuery =
        session.prepare(
            QueryBuilder.insertInto(KEYSPACE, MEMBER_TABLE)
                .value(JsonKey.GROUP_ID, bindMarker())
                .value(JsonKey.USER_ID, bindMarker())
                .value(JsonKey.ROLE, bindMarker())
                .value(JsonKey.STATUS, bindMarker()));

    PreparedStatement updateUserGroupQuery =
        session.prepare(
            QueryBuilder.update(KEYSPACE, USER_GROUP)
                .with(QueryBuilder.addAll(JsonKey.GROUP_ID, bindMarker()))
                .where(eq(JsonKey.USER_ID, bindMarker()))
                .getQueryString());

    PreparedStatement selectGroupMemberQuery =
        session.prepare(
            QueryBuilder.select()
                .all()
                .from(KEYSPACE, MEMBER_TABLE)
                .where(QueryBuilder.eq(JsonKey.USER_ID, QueryBuilder.bindMarker()))
                .and(QueryBuilder.eq(JsonKey.ROLE, QueryBuilder.bindMarker()))
                .and(QueryBuilder.eq(JsonKey.GROUP_ID, QueryBuilder.bindMarker())));

    // link prepared statements to boundstatements
    insertStatement = new BoundStatement(insertGroupQuery);
    selectStatement = new BoundStatement(selectGroupQuery);
    insertMemberStatement = new BoundStatement(insertMemberQuery);
    updateUserGroupStatement = new BoundStatement(updateUserGroupQuery);
    selectGroupMemberStatement = new BoundStatement(selectGroupMemberQuery);
  }

  public static void close() throws Exception {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }
}
