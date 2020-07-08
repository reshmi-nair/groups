package org.sunbird.response;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.exception.BaseException;
import org.sunbird.message.Localizer;
import org.sunbird.message.ResponseCode;
import org.sunbird.request.Request;
import org.sunbird.util.JsonKey;

public class ResponseFactory {
  private static final String msgKeyStr = "message";
  protected static Localizer localizerObject = Localizer.getInstance();

  /**
   * this method will prepare the failure response of the API
   *
   * @param exception
   * @return
   */
  public static Response getFailureMessage(Object exception, Request request) {
    Response response = new Response();
    if (request != null) {
      response.setId(getApiId(request.getPath()));
      response.setVer(request.getVer());
      response.setTs(getCurrentDate());
    }
    if (exception instanceof BaseException) {
      BaseException ex = (BaseException) exception;
      response.setParams(
          createResponseParamObj(
              request, ResponseCode.getResponseCode(ex.getResponseCode()), ex.getMessage()));
      response.setResponseCode(ex.getResponseCode());
    }
    return response;
  }

  public static ResponseParams createResponseParamObj(
      Request request, ResponseCode code, String customMessage) {
    ResponseParams params = new ResponseParams();
    if (code.getCode() != 200) {
      params.setErr(code.name());
      params.setErrmsg(StringUtils.isNotBlank(customMessage) ? customMessage : code.name());
    }
    params.setStatus(ResponseCode.getResponseCode(code.getCode()).name());

    if (request.getHeaders().containsKey(JsonKey.REQUEST_MESSAGE_ID)) {
      ArrayList<String> requestIds =
          (ArrayList<String>) request.getHeaders().get(JsonKey.REQUEST_MESSAGE_ID);
      params.setMsgid(requestIds.get(0));
    }
    return params;
  }

  public static Response getSuccessMessage(Request request) {
    Response response = new Response();
    response.setId(request.getId());
    response.setVer(request.getVer());
    response.setTs(System.currentTimeMillis() + StringUtils.EMPTY);
    return response;
  }

  public static String getApiId(String uri) {
    final String ver = "/" + JsonKey.API_VERSION;
    StringBuilder builder = new StringBuilder();
    if (StringUtils.isNotBlank(uri)) {
      if (uri.contains(ver)) {
        uri = uri.replaceFirst(ver, "api");
      }
      String temVal[] = uri.split("/");
      for (String str : temVal) {
        if (str.matches("[A-Za-z]+")) {
          builder.append(str + ".");
        }
      }
      builder.deleteCharAt(builder.length() - 1);
    }
    return builder.toString();
  }

  public static String getCurrentDate() {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSSZ");
    simpleDateFormat.setLenient(false);
    return simpleDateFormat.format(new Date());
  }
}
