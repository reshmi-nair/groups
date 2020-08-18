package controllers;

import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.request.Request;
import org.sunbird.response.Response;
import org.sunbird.response.ResponseFactory;
import org.sunbird.telemetry.util.TelemetryEvents;
import org.sunbird.telemetry.util.TelemetryWriter;
import org.sunbird.util.JsonKey;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Results;
import utils.module.RequestMapper;

/**
 * this class is used to handle the request and ask from actor and return response on the basis of
 * success and failure to user.
 */
public class ResponseHandler {
  private static Logger logger = LoggerFactory.getLogger(ResponseHandler.class);

  private ResponseHandler() {}

  /**
   * This method will handle all the failure response of Api calls.
   *
   * @param exception
   * @return
   */
  public static Result handleFailureResponse(Object exception, Request request) {
    Result result;
    Response response = ResponseFactory.getFailureMessage(exception, request);
    switch (response.getResponseCode()) {
      case HttpStatus.SC_BAD_REQUEST:
        result = Results.badRequest(Json.toJson(response));
        break;
      case HttpStatus.SC_UNAUTHORIZED:
        result = Results.unauthorized(Json.toJson(response));
        break;
      default:
        result = Results.internalServerError(Json.toJson(response));
        break;
    }
    logTelemetry(response, request);
    return result;
  }
  /**
   * This method will handle all the failure response of Api calls.
   *
   * @param
   * @return
   */
  public static Result handleFailureResponse(Object exception, Http.Request request) {
    RequestMapper requestMapper = new RequestMapper();
    Result result;
    Request sbReq = requestMapper.createSBRequest(request);
    result = handleFailureResponse(exception, sbReq);
    return result;
  }

  /**
   * this method will divert the response on the basis of success and failure
   *
   * @param object
   * @return
   */
  public static Result handleResponse(Object object, Request request) {
    if (object instanceof Response) {
      return handleSuccessResponse((Response) object, request);
    }
    return handleFailureResponse(object, request);
  }

  private static Result handleSuccessResponse(Response response, Request request) {
    String apiId = ResponseFactory.getApiId(request.getPath());
    response.setId(apiId);
    response.setVer(JsonKey.API_VERSION);
    response.setTs(ResponseFactory.getCurrentDate());
    logTelemetry(response, request);
    return Results.ok(Json.toJson(response));
  }

  static void logTelemetry(Response response, Request request) {
    if (null != request.getPath()
        && !(request.getPath().contains("/health")
            || request.getPath().contains("/service/health"))) {
      try {
        long startTime =
            null != request.getTs() ? Long.parseLong(request.getTs()) : System.currentTimeMillis();
        long endTime = System.currentTimeMillis();

        long requestTime = endTime - startTime;
        org.sunbird.request.Request req = new org.sunbird.request.Request();
        Map<String, Object> params = new WeakHashMap<>();
        params.put(JsonKey.URL, request.getPath());
        params.put(JsonKey.LOG_TYPE, JsonKey.API_ACCESS);
        params.put(JsonKey.MESSAGE, "");
        params.put(JsonKey.METHOD, request.getOperation());
        params.put(JsonKey.DURATION, requestTime);
        params.put(JsonKey.STATUS, response.getResponseCode());
        params.put(JsonKey.LOG_LEVEL, JsonKey.INFO);
        req.setRequest(
            generateTelemetryRequestForController(
                TelemetryEvents.LOG.getName(), params, request.getContext()));
        TelemetryWriter.write(req);
      } catch (Exception ex) {
        logger.info("AccessLogFilter:apply Exception in writing telemetry", ex);
      }
    }
  }

  private static Map<String, Object> generateTelemetryRequestForController(
      String eventType, Map<String, Object> params, Map<String, Object> context) {

    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.TELEMETRY_EVENT_TYPE, eventType);
    map.put(JsonKey.CONTEXT, context);
    map.put(JsonKey.PARAMS, params);
    return map;
  }
}
