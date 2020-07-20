package utils;

import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.sunbird.Application;
import org.sunbird.auth.verifier.KeyManager;
import org.sunbird.exception.BaseException;
import org.sunbird.util.*;
import play.api.Environment;
import play.api.inject.ApplicationLifecycle;

/**
 * This class will be called after on application startup. only one instance of this class will be
 * created. StartModule class has responsibility to eager load this class.
 */
@Singleton
public class ApplicationStart {
  /**
   * All one time initialization which required during server startup will fall here.
   *
   * @param lifecycle ApplicationLifecycle
   */
  @Inject
  public ApplicationStart(ApplicationLifecycle lifecycle, Environment environment)
      throws BaseException {
    // instantiate actor system and initialize all the actors
    Application.getInstance().init();
    setEnvironment(environment);
    checkCassandraConnections();
    HttpClientUtil.getInstance();
    ActivityConfigReader.initialize();
    SystemConfigUtil.init();
    // Shut-down hook
    lifecycle.addStopHook(
        () -> {
          return CompletableFuture.completedFuture(null);
        });
    KeyManager.init();
  }

  private void setEnvironment(Environment environment) {
    // TODO: Any env specific work.
    if (environment.asJava().isDev()) {
      // env = ProjectUtil.Environment.dev;
    } else if (environment.asJava().isTest()) {
      // env = ProjectUtil.Environment.qa;
    } else {
      // env = ProjectUtil.Environment.prod;
    }
  }

  private static void checkCassandraConnections() throws BaseException {
    DBUtil.checkCassandraDbConnections();
  }
}
