package org.sunbird.auth.verifier;

import java.io.FileInputStream;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.util.JsonKey;
import org.sunbird.util.helper.PropertiesCache;

public class KeyManager {

  private static Logger logger = LoggerFactory.getLogger(KeyManager.class);
  private static PropertiesCache propertiesCache = PropertiesCache.getInstance();

  private static Map<String, KeyData> keyMap = new HashMap<String, KeyData>();

  public static void init() {
    String basePath = null;
    String keyPrefix = null;
    try {
      logger.info("KeyManager:init: Start");
      basePath = propertiesCache.getProperty(JsonKey.ACCESS_TOKEN_PUBLICKEY_BASEPATH);
      keyPrefix = propertiesCache.getProperty(JsonKey.ACCESS_TOKEN_PUBLICKEY_KEYPREFIX);
      int keyCount =
          Integer.parseInt(propertiesCache.getProperty(JsonKey.ACCESS_TOKEN_PUBLICKEY_KEYCOUNT));
      logger.info(
          "KeyManager:init: basePath: "
              + basePath
              + " keyPrefix: "
              + keyPrefix
              + " keys count: "
              + keyCount);
      for (int i = 1; i <= keyCount; i++) {
        String keyId = keyPrefix + i;
        keyMap.put(keyId, new KeyData(keyId, loadPublicKey(basePath + keyId)));
      }
    } catch (Exception e) {
      logger.error("KeyManager:init: exception in loading publickeys {}", e.getMessage());
    }
  }

  public static KeyData getPublicKey(String keyId) {
    return keyMap.get(keyId);
  }

  private static PublicKey loadPublicKey(String path) throws Exception {
    FileInputStream in = new FileInputStream(path);
    byte[] keyBytes = new byte[in.available()];
    in.read(keyBytes);
    in.close();

    String publicKey = new String(keyBytes, "UTF-8");
    publicKey =
        publicKey.replaceAll("(-+BEGIN PUBLIC KEY-+\\r?\\n|-+END PUBLIC KEY-+\\r?\\n?)", "");
    keyBytes = Base64Util.decode(publicKey.getBytes("UTF-8"), Base64Util.DEFAULT);

    X509EncodedKeySpec X509publicKey = new X509EncodedKeySpec(keyBytes);
    KeyFactory kf = KeyFactory.getInstance("RSA");
    return kf.generatePublic(X509publicKey);
  }
}
