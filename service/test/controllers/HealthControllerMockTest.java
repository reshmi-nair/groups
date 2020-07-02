package controllers;

import static org.junit.Assert.assertEquals;

import akka.actor.ActorRef;
import javax.ws.rs.core.Response;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.exception.BaseException;
import play.mvc.Http;
import play.mvc.Result;
import scala.concurrent.Await;

@Ignore
@RunWith(PowerMockRunner.class)
@PrepareForTest({org.sunbird.Application.class, BaseController.class, ActorRef.class, Await.class})
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*", "javax.security.*"})
public class HealthControllerMockTest extends BaseApplicationTest {

  @Before
  public void before() throws BaseException {
    setup(DummyActor.class);
  }

  @Test
  public void testOnServerHandlerPasses() throws BaseException {
    HealthController healthController = Mockito.mock(HealthController.class);
    PowerMockito.when(
            healthController.createSBRequest(Mockito.any(Http.Request.class), Mockito.anyString()))
        .thenThrow(new RuntimeException("induced due to buggy server impl"));

    Result result = performTest("/service/health", "GET", null);
    System.out.println(getResponseStatus(result));
    assertEquals(Response.Status.fromStatusCode(400).getStatusCode(), getResponseStatus(result));
  }
}
