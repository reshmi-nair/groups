package org.sunbird.actors;

import java.util.Arrays;
import java.util.Map;
import org.sunbird.actor.core.ActorConfig;
import org.sunbird.cache.impl.RedisCache;
import org.sunbird.request.Request;
import org.sunbird.response.Response;
import org.sunbird.util.JsonKey;
import scala.collection.JavaConverters;

@ActorConfig(
  tasks = {"getCache", "setCache", "delCache"},
  dispatcher = "group-dispatcher",
  asyncTasks = {}
)
public class CacheActor extends BaseActor {

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();
    switch (operation) {
      case "setCache":
        setCache(request);
        break;
      case "getCache":
        getCache(request);
        break;
      case "delCache":
        delCache(request);
        break;
      default:
        onReceiveUnsupportedMessage("CacheActor");
    }
  }

  private void setCache(Request request) {
    logger.info("set redis cache for the id {}", request.get(JsonKey.KEY));
    Map<String, Object> req = request.getRequest();
    RedisCache.set(
        (String) req.get(JsonKey.KEY),
        (String) req.get(JsonKey.VALUE),
        (Integer) req.get(JsonKey.TTL));
  }

  private void getCache(Request request) {
    logger.info("get cache for the id {}", request.get(JsonKey.KEY));
    Map<String, Object> req = request.getRequest();
    String value = RedisCache.get((String) req.get(JsonKey.KEY), null, 0);
    Response response = new Response();
    response.put(JsonKey.VALUE, value);
    sender().tell(response, self());
  }

  private void delCache(Request request) {
    logger.info("delete cache from redis for the id {}", request.get(JsonKey.KEY));
    Map<String, Object> req = request.getRequest();
    RedisCache.delete(
        JavaConverters.asScalaIteratorConverter(
                Arrays.asList((String) req.get(JsonKey.KEY)).iterator())
            .asScala()
            .toSeq());
  }
}
