package org.sunbird.actors;

import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.sunbird.message.ResponseCode;
import org.sunbird.response.Response;
import org.sunbird.util.JsonKey;

public abstract class BaseActorTest {

  static ActorSystem system;
  static Map<String, Object> headerMap = new HashMap<>();

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("system");
    setReqId();
  }

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  private static void setReqId() {
    List<String> reqIds = new ArrayList<>();
    reqIds.add("71ef3311-ac58-49a1-872b-7cf28159de83");
    headerMap.put(JsonKey.REQUEST_MESSAGE_ID, reqIds);
  }

  public static Response getCassandraResponse() {
    Response response = new Response();
    response.setResponseCode(ResponseCode.OK.getCode());
    return response;
  }
}
