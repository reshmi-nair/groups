package controllers;

import java.util.concurrent.CompletionStage;
import org.sunbird.exception.BaseException;
import org.sunbird.models.ActorOperations;
import org.sunbird.request.Request;
import play.mvc.Http;
import play.mvc.Result;
import validators.GroupCreateRequestValidator;
import validators.IRequestValidator;

public class CreateGroupController extends BaseController {

  @Override
  protected boolean validate(Request request) throws BaseException {
    IRequestValidator requestValidator = new GroupCreateRequestValidator();
    return requestValidator.validate(request);
  }

  public CompletionStage<Result> createGroup(Http.Request req) {
    Request request = createSBRequest(req, ActorOperations.CREATE_GROUP.getValue());
    return handleRequest(request);
  }
}
