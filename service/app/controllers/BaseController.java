package controllers;

import static controllers.ResponseHandler.handleResponse;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.Application;
import org.sunbird.exception.ActorServiceException;
import org.sunbird.exception.BaseException;
import org.sunbird.exception.ValidationException;
import org.sunbird.message.ResponseCode;
import org.sunbird.request.Request;
import org.sunbird.response.Response;
import org.sunbird.response.ResponseParams;
import org.sunbird.util.ProjectUtil;
import org.sunbird.util.JsonKey;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Http;
import play.mvc.Result;
import scala.compat.java8.FutureConverters;
import scala.concurrent.Future;

/**
 * This controller we can use for writing some common method to handel api request.
 * CompletableFuture: A Future that may be explicitly completed (setting its value and status), and
 * may be used as a CompletionStage, supporting dependent functions and actions that trigger upon
 * its completion. CompletionStage: A stage of a possibly asynchronous computation, that performs an
 * action or computes a value when another CompletionStage completes
 */
public class BaseController extends Controller {
  private static final int WAIT_TIME_VALUE = 30;
  private static final String version = "v1";
  protected ObjectMapper mapper = new ObjectMapper();

  public int getTimeout(Request request) {
    int timeout = WAIT_TIME_VALUE;
    if (request != null && request.getTimeout() > 0) {
      timeout = request.getTimeout();
    }

    return timeout;
  }

  protected ActorRef getActorRef(String operation) {
    return Application.getInstance().getActorRef(operation);
  }

  protected boolean validate(Request request) throws BaseException {
    // All controllers can validate this.
    return false;
  }

  /**
   * this method will take org.sunbird.Request and a validation function and lastly operation(Actor
   * operation) this method is validating the request and , this method is used to handle all the
   * request type which has requestBody
   *
   * @param request
   * @return
   */
  public CompletionStage<Result> handleRequest(Request request) {
    try {
      validate(request);
      return invoke(request);
    } catch (Exception ex) {
      return CompletableFuture.supplyAsync(() -> StringUtils.EMPTY)
          .thenApply(result -> ResponseHandler.handleFailureResponse(ex, request));
    }
  }

  /**
   * Responsible to handle the request and ask from actor
   *
   * @param request
   * @return CompletionStage<Result>
   * @throws BaseException
   */
  public CompletionStage<Result> invoke(Request request) throws BaseException {
    if (request == null) {
      handleResponse(new ValidationException.InvalidRequestData(), request);
    }

    Function<Object, Result> fn =
        new Function<Object, Result>() {
          @Override
          public Result apply(Object object) {
            return handleResponse(object, request);
          }
        };
    Timeout timeout = new Timeout(getTimeout(request), TimeUnit.SECONDS);

    ActorRef actorRef = getActorRef(request.getOperation());
    if (actorRef != null) {
      Future<Object> future = Patterns.ask(actorRef, request, timeout);
      return FutureConverters.toJava(future).thenApplyAsync(fn);
    } else {
      return CompletableFuture.supplyAsync(
          () -> handleResponse(new ActorServiceException.InvalidOperationName(null), request));
    }
  }

  private Request createSBRequest(play.mvc.Http.Request httpReq) {
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

  public Request createSBRequest(play.mvc.Http.Request httpReq, String operation) {
    Request request = createSBRequest(httpReq);
    request.setOperation(operation);
    return request;
  }
  
  /**
   * This method will create failure response
   *
   * @param request Request
   * @param code ResponseCode
   * @param headerCode ResponseCode
   * @return Response
   */
  public static Response createFailureResponse(
    Http.Request request, ResponseCode code, ResponseCode headerCode) {
    
    Response response = new Response();
    response.setVer(getApiVersion(request.path()));
    response.setId(getApiResponseId(request));
    response.setTs(ProjectUtil.getFormattedDate());
    response.setResponseCode(headerCode.getCode());
    response.setParams(createResponseParamObj(code, null, request.flash().get(JsonKey.REQUEST_ID)));
    return response;
  }
  
  public static ResponseParams createResponseParamObj(
    ResponseCode code, String customMessage, String requestId) {
    ResponseParams params = new ResponseParams();
    if (code.getCode() != 200) {
      params.setErr(code.getErrorCode());
      params.setErrmsg(
        StringUtils.isNotBlank(customMessage) ? customMessage : code.getErrorMessage());
    }
    params.setMsgid(requestId);
    params.setStatus(ResponseCode.getHeaderResponseCode(code.getResponseCode()).name());
    return params;
  }
  
  /**
   * This method will provide api version.
   *
   * @param request String
   * @return String
   */
  public static String getApiVersion(String request) {
    
    return request.split("[/]")[1];
  }
  
  /**
   * Method to get API response Id
   *
   * @param request play.mvc.Http.Request
   * @return String
   */
  private static String getApiResponseId(Http.Request request) {
    
    String val = "";
    if (request != null) {
      String path = request.path();
      if (request.method().equalsIgnoreCase(ProjectUtil.Method.GET.name())) {
        val = getResponseId(path);
        if (StringUtils.isBlank(val)) {
          String[] splitedpath = path.split("[/]");
          path = removeLastValue(splitedpath);
          val = getResponseId(path);
        }
      } else {
        val = getResponseId(path);
      }
      if (StringUtils.isBlank(val)) {
        val = getResponseId(path);
        if (StringUtils.isBlank(val)) {
          String[] splitedpath = path.split("[/]");
          path = removeLastValue(splitedpath);
          val = getResponseId(path);
        }
      }
    }
    return val;
  }
  
  /**
   * Method to get the response id on basis of request path.
   *
   * @param requestPath
   * @return
   */
  public static String getResponseId(String requestPath) {
    
    String path = requestPath;
    final String ver = "/" + version;
    final String ver2 = "/" + JsonKey.VERSION_2;
    path = path.trim();
    StringBuilder builder = new StringBuilder("");
    if (path.startsWith(ver) || path.startsWith(ver2)) {
      String requestUrl = (path.split("\\?"))[0];
      if (requestUrl.contains(ver)) {
        requestUrl = requestUrl.replaceFirst(ver, "api");
      } else if (requestUrl.contains(ver2)) {
        requestUrl = requestUrl.replaceFirst(ver2, "api");
      }
      String[] list = requestUrl.split("/");
      for (String str : list) {
        if (str.matches("[A-Za-z]+")) {
          builder.append(str).append(".");
        }
      }
      builder.deleteCharAt(builder.length() - 1);
    } else {
      if ("/health".equalsIgnoreCase(path)) {
        builder.append("api.all.health");
      }
    }
    return builder.toString();
  }
  
  /**
   * Method to remove last value
   *
   * @param splited String []
   * @return String
   */
  private static String removeLastValue(String splited[]) {
    
    StringBuilder builder = new StringBuilder();
    if (splited != null && splited.length > 0) {
      for (int i = 1; i < splited.length - 1; i++) {
        builder.append("/" + splited[i]);
      }
    }
    return builder.toString();
  }
}
