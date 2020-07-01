package controllers;

import java.util.concurrent.CompletionStage;
import org.sunbird.exception.BaseException;
import org.sunbird.request.Request;
import play.mvc.Http;
import play.mvc.Result;

/** This controller class will responsible to check health of the services. */
public class HealthController extends BaseController {
  // Service name must be "service" for the DevOps monitoring.
  private static final String service = "service";

  /**
   * This action method is responsible for checking Health.
   *
   * @return a CompletableFuture of success response
   */
  public CompletionStage<Result> getHealth() throws BaseException {
    Request req = new Request("health"); // Get API
    return handleRequest(req);
  }

  /**
   * This action method is responsible to check service health
   *
   * @return a CompletableFuture of success response
   */
  public CompletionStage<Result> getServiceHealth(String serviceName, Http.Request req)
      throws BaseException {
    Request request = createSBRequest(req, "health");
    request.getContext().put("service", serviceName);
    return handleRequest(request);
  }
}
