package utils.module;

import com.fasterxml.jackson.databind.ObjectMapper;
import controllers.ResponseHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.exception.BaseException;
import org.sunbird.message.IResponseMessage;
import org.sunbird.message.ResponseCode;
import org.sunbird.request.HeaderParam;
import org.sunbird.service.UserService;
import org.sunbird.service.UserServiceImpl;
import org.sunbird.util.JsonKey;
import org.sunbird.util.ProjectUtil;
import org.sunbird.util.SystemConfigUtil;
import org.sunbird.util.helper.PropertiesCache;
import play.http.ActionCreator;
import play.mvc.Action;
import play.mvc.Http;
import play.mvc.Result;

public class OnRequestHandler implements ActionCreator {

  private static Logger logger = LoggerFactory.getLogger(OnRequestHandler.class);
  public static boolean isServiceHealthy = true;
  private static String custodianOrgHashTagId;
  private UserService userService = UserServiceImpl.getInstance();
  private ObjectMapper mapper = new ObjectMapper();

  @Override
  public Action createAction(Http.Request request, Method method) {
    return new Action.Simple() {
      @Override
      public CompletionStage<Result> call(Http.Request request) {
        request.getHeaders();
        CompletionStage<Result> result = checkForServiceHealth(request);
        if (result != null) return result;
        request.flash().put(JsonKey.USER_ID, null);
        String message = RequestInterceptor.verifyRequestData(request);
        initializeContext(request, message);
        if (!JsonKey.USER_UNAUTH_STATES.contains(message)) {
          request.flash().put(JsonKey.USER_ID, message);
          request.flash().put(JsonKey.IS_AUTH_REQ, "false");
          result = delegate.call(request);
        } else if (JsonKey.UNAUTHORIZED.equals(message)) {
          result = onDataValidationError(request, message);
        } else {
          result = delegate.call(request);
        }
        return result.thenApply(res -> res.withHeader("Access-Control-Allow-Origin", "*"));
      }
    };
  }

  public static CompletionStage<Result> checkForServiceHealth(Http.Request request) {
    if (Boolean.parseBoolean((ProjectUtil.getConfigValue(JsonKey.SUNBIRD_HEALTH_CHECK_ENABLE)))
        && !request.path().endsWith(JsonKey.HEALTH)) {
      if (!isServiceHealthy) {
        ResponseCode headerCode = ResponseCode.SERVICE_UNAVAILABLE;
        Result result =
            ResponseHandler.handleFailureResponse(
                new BaseException(
                    ResponseCode.CLIENT_ERROR.getErrorCode(),
                    headerCode.getErrorMessage(),
                    ResponseCode.UNAUTHORIZED.getResponseCode()),
                request);
        return CompletableFuture.completedFuture(result);
      }
    }
    return null;
  }

  /**
   * This method will do request data validation for GET method only. As a GET request user must
   * send some key in header.
   *
   * @param request Request
   * @param errorMessage String
   * @return CompletionStage<Result>
   */
  public CompletionStage<Result> onDataValidationError(Http.Request request, String errorMessage) {
    logger.error("Data error found--" + errorMessage);
    ResponseCode code = ResponseCode.getResponse(errorMessage);
    Result result =
        ResponseHandler.handleFailureResponse(
            new BaseException(
                ResponseCode.CLIENT_ERROR.getErrorCode(),
                code.getErrorMessage(),
                ResponseCode.UNAUTHORIZED.getResponseCode()),
            request);
    return CompletableFuture.completedFuture(result);
  }

  /**
   * Set the Context paramter to the request
   *
   * @param httpReq
   * @param userId
   */
  private void initializeContext(Http.Request httpReq, String userId) {
    try {
      Map<String, Object> requestContext = new WeakHashMap<>();
      String env = getEnv(httpReq);
      requestContext.put(JsonKey.ENV, env);
      requestContext.put(JsonKey.REQUEST_TYPE, JsonKey.API_CALL);
      Optional<String> optionalChannel = httpReq.getHeaders().get(HeaderParam.CHANNEL_ID.getName());
      String channel;
      if (optionalChannel.isPresent()) {
        channel = optionalChannel.get();
      } else {
        channel = getCustodianOrgHashTagId();
      }
      requestContext.put(JsonKey.CHANNEL, channel);
      requestContext.put(
          JsonKey.REQUEST_ID, httpReq.getHeaders().get(JsonKey.REQUEST_MESSAGE_ID).get());
      requestContext.putAll(cacheTelemetryPdata());
      Optional<String> optionalAppId = httpReq.getHeaders().get(HeaderParam.X_APP_ID.getName());
      if (optionalAppId.isPresent()) {
        requestContext.put(JsonKey.APP_ID, optionalAppId.get());
      }
      Optional<String> optionalDeviceId =
          httpReq.getHeaders().get(HeaderParam.X_Device_ID.getName());
      if (optionalDeviceId.isPresent()) {
        requestContext.put(JsonKey.DEVICE_ID, optionalDeviceId.get());
      }
      if (null != userId) {
        requestContext.put(JsonKey.ACTOR_ID, userId);
        requestContext.put(JsonKey.ACTOR_TYPE, StringUtils.capitalize(JsonKey.USER));
      } else {
        Optional<String> optionalConsumerId =
            httpReq.getHeaders().get(HeaderParam.X_Consumer_ID.getName());
        String consumerId;
        if (optionalConsumerId.isPresent()) {
          consumerId = optionalConsumerId.get();
        } else {
          consumerId = JsonKey.DEFAULT_CONSUMER_ID;
        }
        requestContext.put(JsonKey.ACTOR_ID, consumerId);
        requestContext.put(JsonKey.ACTOR_TYPE, StringUtils.capitalize(JsonKey.CONSUMER));
      }
      Map<String, Object> map = new WeakHashMap<>();
      map.put(JsonKey.CONTEXT, requestContext);
      httpReq.flash().put(JsonKey.CONTEXT, mapper.writeValueAsString(map));
    } catch (Exception ex) {
      logger.error("Error process set request context" + ex.getMessage());
      throw new BaseException(
          IResponseMessage.SERVER_ERROR,
          IResponseMessage.INTERNAL_ERROR,
          ResponseCode.SERVER_ERROR.getCode());
    }
  }

  private String getCustodianOrgHashTagId() {
    if (null != custodianOrgHashTagId) {
      return custodianOrgHashTagId;
    }
    synchronized (OnRequestHandler.class) {
      if (custodianOrgHashTagId == null) {
        try {
          // Get hash tag ID of custodian org
          Map<String, Object> custodianOrgDetails = SystemConfigUtil.getCustodianOrgDetails();
          if (null != custodianOrgDetails && !custodianOrgDetails.isEmpty()) {
            custodianOrgHashTagId = (String) custodianOrgDetails.get(JsonKey.HASH_TAG_ID);
          } else {
            custodianOrgHashTagId = "";
          }

        } catch (Exception ex) {
          custodianOrgHashTagId = "";
        }
      }
    }
    return custodianOrgHashTagId;
  }

  private static Map<String, Object> cacheTelemetryPdata() {
    Map<String, Object> telemetryPdata = new HashMap<>();
    telemetryPdata.put("telemetry_pdata_id", PropertiesCache.getConfigValue("telemetry_pdata_id"));
    telemetryPdata.put(
        "telemetry_pdata_pid", PropertiesCache.getConfigValue("telemetry_pdata_pid"));
    telemetryPdata.put(
        "telemetry_pdata_ver", PropertiesCache.getConfigValue("telemetry_pdata_ver"));
    return telemetryPdata;
  }

  private static String getEnv(Http.Request request) {
    String uri = request.uri();
    String env = "";
    if (uri.startsWith("/v1/group")) {
      env = JsonKey.USER;
    }
    return env;
  }
}
