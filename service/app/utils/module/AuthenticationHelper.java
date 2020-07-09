package utils.module;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.sso.SSOManager;
import org.sunbird.sso.SSOServiceFactory;
import org.sunbird.util.JsonKey;
import org.sunbird.util.helper.PropertiesCache;

/**
 * This class will handle all the method related to authentication. For example verifying user
 * access token, creating access token after success login.
 *
 * @author Manzarul
 */
public class AuthenticationHelper {
  private static Logger logger = LoggerFactory.getLogger(AuthenticationHelper.class);

  private static boolean ssoEnabled =
      ((PropertiesCache.getInstance().getProperty(JsonKey.SSO_PUBLIC_KEY) != null)
          && (Boolean.parseBoolean(
              PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED))));

  /**
   * This method will verify the incoming user access token against store data base /cache. If token
   * is valid then it would be associated with some user id. In case of token matched it will
   * provide user id. else will provide empty string.
   *
   * @param token String
   * @return String
   */
  @SuppressWarnings("unchecked")
  public static String verifyUserAccesToken(String token) {
    SSOManager ssoManager = SSOServiceFactory.getInstance();
    String userId = JsonKey.UNAUTHORIZED;
    try {
      if (ssoEnabled) {
        userId = ssoManager.verifyToken(token);
      }
    } catch (Exception e) {
      logger.error("invalid auth token =" + token, e);
    }
    return userId;
  }
}
