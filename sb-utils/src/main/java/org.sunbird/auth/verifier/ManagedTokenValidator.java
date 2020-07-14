package org.sunbird.auth.verifier;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.util.JsonKey;

public class ManagedTokenValidator {

  private static Logger logger = LoggerFactory.getLogger(ManagedTokenValidator.class);
  private static ObjectMapper mapper = new ObjectMapper();

  /**
   * managedtoken is validated and requestedByUserID, requestedForUserID values are validated
   * aganist the managedEncToken
   *
   * @param managedEncToken
   * @param requestedByUserId
   * @param requestedForUserId
   * @return
   */
  public static String verify(
      String managedEncToken, String requestedByUserId, String requestedForUserId) {
    boolean isValid = false;
    String managedFor = JsonKey.UNAUTHORIZED;
    try {
      String[] tokenElements = managedEncToken.split("\\.");
      String header = tokenElements[0];
      String body = tokenElements[1];
      String signature = tokenElements[2];
      String payLoad = header + JsonKey.DOT_SEPARATOR + body;
      Map<Object, Object> headerData =
          mapper.readValue(new String(decodeFromBase64(header)), Map.class);
      String keyId = headerData.get("kid").toString();
      logger.info("ManagedTokenValidator:verify: keyId: " + keyId);
      Map<String, String> tokenBody =
          mapper.readValue(new String(decodeFromBase64(body)), Map.class);
      String parentId = tokenBody.get(JsonKey.PARENT_ID);
      String muaId = tokenBody.get(JsonKey.SUB);
      logger.info(
          "ManagedTokenValidator: parent uuid: "
              + parentId
              + " managedBy uuid: "
              + muaId
              + " requestedByUserID: "
              + requestedByUserId
              + " requestedForUserId: "
              + requestedForUserId);
      logger.info("ManagedTokenValidator: key modified value: " + keyId);
      isValid =
          CryptoUtil.verifyRSASign(
              payLoad,
              decodeFromBase64(signature),
              KeyManager.getPublicKey(keyId).getPublicKey(),
              JsonKey.SHA_256_WITH_RSA);
      isValid &=
          parentId.equalsIgnoreCase(requestedByUserId)
              && muaId.equalsIgnoreCase(requestedForUserId);
      if (isValid) {
        managedFor = muaId;
      }
    } catch (Exception ex) {
      logger.error("Exception in ManagedTokenValidator: verify ", ex.getMessage());
    }

    return managedFor;
  }

  /**
   * managedtoken is validated and requestedByUserID values are validated
   * aganist the managedEncToken
   *
   * @param managedEncToken
   * @param requestedByUserId
   * @return
   */
  public static String verify(
          String managedEncToken, String requestedByUserId) {
    boolean isValid = false;
    String managedFor = JsonKey.UNAUTHORIZED;
    try {
      String[] tokenElements = managedEncToken.split("\\.");
      String header = tokenElements[0];
      String body = tokenElements[1];
      String signature = tokenElements[2];
      String payLoad = header + JsonKey.DOT_SEPARATOR + body;
      Map<Object, Object> headerData =
              mapper.readValue(new String(decodeFromBase64(header)), Map.class);
      String keyId = headerData.get("kid").toString();
      logger.info("ManagedTokenValidator: verify: keyId: " + keyId);
      Map<String, String> tokenBody =
              mapper.readValue(new String(decodeFromBase64(body)), Map.class);
      String parentId = tokenBody.get(JsonKey.PARENT_ID);
      String muaId = tokenBody.get(JsonKey.SUB);
      logger.info(
              "ManagedTokenValidator: parent uuid: "
                      + parentId
                      + " managedBy uuid: "
                      + muaId
                      + " requestedByUserID: "
                      + requestedByUserId);
      logger.info("ManagedTokenValidator: key modified value: " + keyId);
      isValid = CryptoUtil.verifyRSASign(
                      payLoad,
                      decodeFromBase64(signature),
                      KeyManager.getPublicKey(keyId).getPublicKey(),
                      JsonKey.SHA_256_WITH_RSA);
      isValid &=
              parentId.equalsIgnoreCase(requestedByUserId);
      if (isValid) {
        managedFor = muaId;
      }
    } catch (Exception ex) {
      logger.error("Exception in ManagedTokenValidator: verify {} ", ex.getMessage());
    }

    return managedFor;
  }

  private static byte[] decodeFromBase64(String data) {
    return Base64Util.decode(data, 11);
  }
}
