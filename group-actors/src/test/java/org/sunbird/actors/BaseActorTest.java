package org.sunbird.actors;

import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.sunbird.Application;
import org.sunbird.cache.impl.RedisCache;
import org.sunbird.message.ResponseCode;
import org.sunbird.response.Response;
import org.sunbird.util.JsonKey;

@PrepareForTest({Application.class, RedisCache.class})
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

  public void mockCacheActor() throws Exception {
    ActorSystem actorSystem = ActorSystem.create("system");
    Props props = Props.create(CacheActor.class);
    ActorRef actorRef = actorSystem.actorOf(props);
    Application app = PowerMockito.mock(Application.class);
    PowerMockito.mockStatic(Application.class);
    when(Application.getInstance()).thenReturn(app);
    when(app.getActorRef(Mockito.anyString())).thenReturn(actorRef);
    PowerMockito.mockStatic(RedisCache.class);
    doNothing()
        .when(RedisCache.class, "set", Mockito.anyString(), Mockito.anyString(), Mockito.anyInt());
    doNothing().when(RedisCache.class, "delete", Mockito.anyObject());
    when(RedisCache.get(Mockito.anyString(), Mockito.anyObject(), Mockito.anyInt())).thenReturn("");
  }
}
