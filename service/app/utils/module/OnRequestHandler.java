package utils.module;

import controllers.ResponseHandler;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.exception.BaseException;
import org.sunbird.message.ResponseCode;
import org.sunbird.util.JsonKey;
import play.http.ActionCreator;
import play.mvc.Action;
import play.mvc.Http;
import play.mvc.Result;

public class OnRequestHandler implements ActionCreator {

  private static Logger logger = LoggerFactory.getLogger(OnRequestHandler.class);

  @Override
  public Action createAction(Http.Request request, Method method) {

    return new Action.Simple() {
      @Override
      public CompletionStage<Result> call(Http.Request request) {
        CompletionStage<Result> result = null;
        request.flash().put(JsonKey.USER_ID, null);
        request.getHeaders();
        String message = RequestInterceptor.verifyRequestData(request);
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
}
