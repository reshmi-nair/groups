package utils.module;

import akka.util.ByteString;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.Executor;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import play.libs.streams.Accumulator;
import play.mvc.EssentialAction;
import play.mvc.EssentialFilter;
import play.mvc.Result;

public class AccessLogFilter extends EssentialFilter {
  private static final Logger logger = LoggerFactory.getLogger(AccessLogFilter.class);

  private final Executor executor;
  private ObjectMapper objectMapper = new ObjectMapper();

  @Inject
  public AccessLogFilter(Executor executor) {
    super();
    this.executor = executor;
  }

  @Override
  public EssentialAction apply(EssentialAction next) {
    return EssentialAction.of(
        request -> {
          long startTime = System.currentTimeMillis();
          Accumulator<ByteString, Result> accumulator = next.apply(request);
          return accumulator.map(
              result -> {
                MDC.clear();
                return result;
              },
              executor);
        });
  }
}
