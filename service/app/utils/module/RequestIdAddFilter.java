package utils.module;

import akka.stream.Materializer;
import com.google.inject.Inject;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.util.JsonKey;
import play.mvc.Filter;
import play.mvc.Http;
import play.mvc.Result;

public class RequestIdAddFilter extends Filter {
  private static final Logger log = LoggerFactory.getLogger(RequestIdAddFilter.class);

  @Inject
  public RequestIdAddFilter(Materializer materializer) {
    super(materializer);
  }

  @Override
  public CompletionStage<Result> apply(
      Function<Http.RequestHeader, CompletionStage<Result>> nextFilter,
      Http.RequestHeader requestHeader) {
    Optional<String> requestIdHeader = requestHeader.getHeaders().get(JsonKey.REQUEST_MESSAGE_ID);
    String reqId =
        requestIdHeader.isPresent() ? requestIdHeader.get() : UUID.randomUUID().toString();
    requestHeader.getHeaders().addHeader(JsonKey.REQUEST_MESSAGE_ID, reqId);
    return nextFilter.apply(requestHeader);
  }
}
