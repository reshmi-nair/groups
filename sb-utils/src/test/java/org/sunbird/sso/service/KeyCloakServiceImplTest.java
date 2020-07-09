package org.sunbird.sso.service;

import static org.powermock.api.mockito.PowerMockito.*;

import java.lang.reflect.Constructor;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.keycloak.admin.client.Keycloak;
import org.powermock.api.mockito.PowerMockito;
import org.sunbird.exception.BaseException;
import org.sunbird.sso.KeyCloakConnectionProvider;
import org.sunbird.sso.SSOManager;
import org.sunbird.sso.SSOServiceFactory;

public class KeyCloakServiceImplTest extends BaseHttpTest {

  private SSOManager keyCloakService = SSOServiceFactory.getInstance();
  private static Class t = null;

  @BeforeClass
  public static void init() {
    try {
      t = Class.forName("org.sunbird.sso.SSOServiceFactory");
    } catch (ClassNotFoundException e) {
    }
    Keycloak kcp = mock(Keycloak.class);
    PowerMockito.mockStatic(KeyCloakConnectionProvider.class);
    try {
      doReturn(kcp).when(KeyCloakConnectionProvider.class, "getConnection");
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(
          "Failed in initialization of mock rules, underlying error: " + e.getLocalizedMessage());
    }
  }

  @Test
  public void testNewInstanceSucccess() {
    Exception exp = null;
    try {
      Constructor<SSOServiceFactory> constructor = t.getDeclaredConstructor();
      constructor.setAccessible(true);
      SSOServiceFactory application = constructor.newInstance();
      Assert.assertNotNull(application);
    } catch (Exception e) {
      exp = e;
    }
    Assert.assertNull(exp);
  }

  @Test(expected = BaseException.class)
  public void testVerifyTokenSuccess() {
    keyCloakService.verifyToken(
        "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI5emhhVnZDbl81OEtheHpldHBzYXNZQ2lEallkemJIX3U2LV93SDk4SEc0In0.eyJqdGkiOiI5ZmQzNzgzYy01YjZmLTQ3OWQtYmMzYy0yZWEzOGUzZmRmYzgiLCJleHAiOjE1MDUxMTQyNDYsIm5iZiI6MCwiaWF0IjoxNTA1MTEzNjQ2LCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjgwODAvYXV0aC9yZWFsbXMvbWFzdGVyIiwiYXVkIjoic2VjdXJpdHktYWRtaW4tY29uc29sZSIsInN1YiI6ImIzYTZkMTY4LWJjZmQtNDE2MS1hYzVmLTljZjYyODIyNzlmMyIsInR5cCI6IkJlYXJlciIsImF6cCI6InNlY3VyaXR5LWFkbWluLWNvbnNvbGUiLCJub25jZSI6ImMxOGVlMDM2LTAyMWItNGVlZC04NWVhLTc0MjMyYzg2ZmI4ZSIsImF1dGhfdGltZSI6MTUwNTExMzY0Niwic2Vzc2lvbl9zdGF0ZSI6ImRiZTU2NDlmLTY4MDktNDA3NS05Njk5LTVhYjIyNWMwZTkyMiIsImFjciI6IjEiLCJhbGxvd2VkLW9yaWdpbnMiOltdLCJyZXNvdXJjZV9hY2Nlc3MiOnt9LCJuYW1lIjoiTWFuemFydWwgaGFxdWUiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJ0ZXN0MTIzNDU2NyIsImdpdmVuX25hbWUiOiJNYW56YXJ1bCBoYXF1ZSIsImVtYWlsIjoidGVzdDEyM0B0LmNvbSJ9.Xdjqe16MSkiR94g-Uj_pVZ2L3gnIdKpkJ6aB82W_w_c3yEmx1mXYBdkxe4zMz3ks4OX_PWwSFEbJECHcnujUwF6Ula0xtXTfuESB9hFyiWHtVAhuh5UlCCwPnsihv5EqK6u-Qzo0aa6qZOiQK3Zo7FLpnPUDxn4yHyo3mRZUiWf76KTl8PhSMoXoWxcR2vGW0b-cPixILTZPV0xXUZoozCui70QnvTgOJDWqr7y80EWDkS4Ptn-QM3q2nJlw63mZreOG3XTdraOlcKIP5vFK992dyyHlYGqWVzigortS9Ah4cprFVuLlX8mu1cQvqHBtW-0Dq_JlcTMaztEnqvJ6XA");
  }
}
