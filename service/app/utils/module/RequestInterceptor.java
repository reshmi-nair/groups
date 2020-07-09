package utils.module;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.auth.verifier.ManagedTokenValidator;
import org.sunbird.request.HeaderParam;
import org.sunbird.util.JsonKey;
import play.mvc.Http;

/**
 * Request interceptor responsible to authenticated HTTP requests
 *
 * @author Amit Kumar
 */
public class RequestInterceptor {
  static Logger logger = LoggerFactory.getLogger(RequestInterceptor.class);

  private RequestInterceptor() {}

  private static String getUserRequestedFor(Http.Request request) {
    String requestedForUserID = null;
    JsonNode jsonBody = request.body().asJson();
    if (!(jsonBody == null)) { // for search and update and create_mui api's
      if (!(jsonBody.get(JsonKey.REQUEST).get(JsonKey.USER_ID) == null)) {
        requestedForUserID = jsonBody.get(JsonKey.REQUEST).get(JsonKey.USER_ID).asText();
      }
    } else { // for read-api
      String uuidSegment = null;
      Path path = Paths.get(request.uri());
      if (request.queryString().isEmpty()) {
        uuidSegment = path.getFileName().toString();
      } else {
        String[] queryPath = path.getFileName().toString().split("\\?");
        uuidSegment = queryPath[0];
      }
      try {
        requestedForUserID = UUID.fromString(uuidSegment).toString();
      } catch (IllegalArgumentException iae) {
        logger.error("Perhaps this is another API, like search that doesn't carry user id.");
      }
    }
    return requestedForUserID;
  }

  /**
   * Authenticates given HTTP request context
   *
   * @param request HTTP play request
   * @return User or Client ID for authenticated request. For unauthenticated requests, UNAUTHORIZED
   *     is returned release-3.0.0 on-wards validating managedBy token.
   */
  public static String verifyRequestData(Http.Request request) {
    String clientId = JsonKey.UNAUTHORIZED;
    request.flash().put(JsonKey.MANAGED_FOR, null);
    Optional<String> accessToken = request.header(HeaderParam.X_Authenticated_User_Token.getName());
    Optional<String> authClientId = request.header(HeaderParam.X_Authenticated_Client_Id.getName());
    // The API must be invoked with either access token or client token.
    if (accessToken.isPresent()) {
      clientId = AuthenticationHelper.verifyUserAccesToken(accessToken.get());
      if (!JsonKey.USER_UNAUTH_STATES.contains(clientId)) {
        // Now we have some valid token, next verify if the token is matching the request.
        String requestedForUserID = getUserRequestedFor(request);
        if (StringUtils.isNotEmpty(requestedForUserID) && !requestedForUserID.equals(clientId)) {
          // LUA - MUA user combo, check the 'for' token and its parent, child identifiers
          Optional<String> forTokenHeader =
              request.header(HeaderParam.X_Authenticated_For.getName());
          String managedAccessToken = forTokenHeader.isPresent() ? forTokenHeader.get() : "";
          if (StringUtils.isNotEmpty(managedAccessToken)) {
            String managedFor =
                ManagedTokenValidator.verify(managedAccessToken, clientId, requestedForUserID);
            if (!JsonKey.USER_UNAUTH_STATES.contains(managedFor)) {
              request.flash().put(JsonKey.MANAGED_FOR, managedFor);
            } else {
              clientId = JsonKey.UNAUTHORIZED;
            }
          }
        } else {
          logger.info("Ignoring x-authenticated-for token...");
        }
      }
    }
    return clientId;
  }
}
