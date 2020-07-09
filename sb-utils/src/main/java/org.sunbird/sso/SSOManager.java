/** */
package org.sunbird.sso;

/** @author Manzarul This interface will handle all call related to single sign out. */
public interface SSOManager {

  /**
   * This method will verify user access token and provide userId if token is valid. in case of
   * invalid access token it will throw ProjectCommon exception with 401.
   *
   * @param token String JWT access token
   * @return String
   */
  String verifyToken(String token);

  /**
   * This method will verify user access token and provide userId if token is valid. in case of
   * invalid access token it will throw ProjectCommon exception with 401.
   *
   * @param token String JWT access token
   * @param url token will be validated against this url
   * @return String
   */
  String verifyToken(String token, String url);
}
