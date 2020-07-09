package org.sunbird.sso.service;

import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.keycloak.RSATokenVerifier;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.representations.AccessToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.exception.BaseException;
import org.sunbird.message.ResponseCode;
import org.sunbird.sso.KeyCloakConnectionProvider;
import org.sunbird.sso.SSOManager;
import org.sunbird.util.JsonKey;

/**
 * Single sign out service implementation with Key Cloak.
 *
 * @author Manzarul
 */
public class KeyCloakServiceImpl implements SSOManager {

  private Logger logger = LoggerFactory.getLogger(KeyCloakServiceImpl.class);

  private Keycloak keycloak = KeyCloakConnectionProvider.getConnection();
  private static final String URL =
      KeyCloakConnectionProvider.SSO_URL
          + "realms/"
          + KeyCloakConnectionProvider.SSO_REALM
          + "/protocol/openid-connect/token";

  private static PublicKey SSO_PUBLIC_KEY = null;

  public PublicKey getPublicKey() {
    if (null == SSO_PUBLIC_KEY) {
      SSO_PUBLIC_KEY =
          new KeyCloakRsaKeyFetcher()
              .getPublicKeyFromKeyCloak(
                  KeyCloakConnectionProvider.SSO_URL, KeyCloakConnectionProvider.SSO_REALM);
    }
    return SSO_PUBLIC_KEY;
  }

  @Override
  public String verifyToken(String accessToken) {
    return verifyToken(accessToken, null);
  }

  /**
   * This method will generate Public key form keycloak realm publickey String
   *
   * @param publicKeyString String
   * @return PublicKey
   */
  private PublicKey toPublicKey(String publicKeyString) {
    try {
      byte[] publicBytes = Base64.getDecoder().decode(publicKeyString);
      X509EncodedKeySpec keySpec = new X509EncodedKeySpec(publicBytes);
      KeyFactory keyFactory = KeyFactory.getInstance("RSA");
      return keyFactory.generatePublic(keySpec);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public String verifyToken(String accessToken, String url) {

    try {
      PublicKey publicKey = getPublicKey();
      if (publicKey == null) {
        logger.info(
            "KeyCloakServiceImpl: SSO_PUBLIC_KEY is NULL. Keycloak server may need to be started. Read value from environment variable.");
        publicKey = toPublicKey(System.getenv(JsonKey.SSO_PUBLIC_KEY));
      }
      if (publicKey != null) {
        String ssoUrl = (url != null ? url : KeyCloakConnectionProvider.SSO_URL);
        AccessToken token =
            RSATokenVerifier.verifyToken(
                accessToken,
                publicKey,
                ssoUrl + "realms/" + KeyCloakConnectionProvider.SSO_REALM,
                true,
                true);
        logger.info(
            token.getId()
                + " "
                + token.issuedFor
                + " "
                + token.getProfile()
                + " "
                + token.getSubject()
                + " Active: "
                + token.isActive()
                + "  isExpired: "
                + token.isExpired()
                + " "
                + token.issuedNow().getExpiration());
        String tokenSubject = token.getSubject();
        if (StringUtils.isNotBlank(tokenSubject)) {
          int pos = tokenSubject.lastIndexOf(":");
          return tokenSubject.substring(pos + 1);
        }
        return token.getSubject();
      } else {
        logger.error("KeyCloakServiceImpl:verifyToken: SSO_PUBLIC_KEY is NULL.");
        throw new BaseException(
            ResponseCode.keyCloakDefaultError.getErrorCode(),
            ResponseCode.keyCloakDefaultError.getErrorMessage(),
            ResponseCode.keyCloakDefaultError.getResponseCode());
      }
    } catch (Exception e) {
      logger.error(
          "KeyCloakServiceImpl:verifyToken: Exception occurred with message = " + e.getMessage());
      throw new BaseException(
          ResponseCode.unAuthorized.getErrorCode(),
          ResponseCode.unAuthorized.getErrorMessage(),
          ResponseCode.UNAUTHORIZED.getResponseCode());
    }
  }
}
