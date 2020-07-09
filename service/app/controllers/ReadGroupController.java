package controllers;

import java.util.concurrent.CompletionStage;
import org.sunbird.exception.BaseException;
import org.sunbird.models.ActorOperations;
import org.sunbird.request.Request;
import org.sunbird.util.JsonKey;
import play.mvc.Http;
import play.mvc.Result;
import validators.GroupReadRequestValidator;
import validators.IRequestValidator;

public class ReadGroupController extends BaseController {
  @Override
  protected boolean validate(Request request) throws BaseException {
    IRequestValidator requestValidator = new GroupReadRequestValidator();
    return requestValidator.validate(request);
  }

  public CompletionStage<Result> readGroup(String groupId, Http.Request req) {
    Request request = createSBRequest(req, ActorOperations.READ_GROUP.getValue());
    request.getRequest().put(JsonKey.GROUP_ID, groupId);
    return handleRequest(request);
  }
}
