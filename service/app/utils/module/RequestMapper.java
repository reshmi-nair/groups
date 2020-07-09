package utils.module;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sunbird.request.Request;
import org.sunbird.util.JsonKey;
import play.libs.Json;

public class RequestMapper {

  public Request createSBRequest(play.mvc.Http.Request httpReq) {
    // Copy body

    JsonNode requestData = httpReq.body().asJson();
    if (requestData == null || requestData.isMissingNode()) {
      requestData = JsonNodeFactory.instance.objectNode();
    }

    // Copy headers
    ObjectNode headerData = Json.mapper().valueToTree(httpReq.getHeaders().toMap());
    ((ObjectNode) requestData).set("headers", headerData);

    Request request = Json.fromJson(requestData, Request.class);
    request.getContext().put(JsonKey.USER_ID, httpReq.flash().get(JsonKey.USER_ID));
    request.setPath(httpReq.path());

    return request;
  }
}
