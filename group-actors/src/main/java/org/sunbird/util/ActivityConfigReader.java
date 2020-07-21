package org.sunbird.util;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ActivityConfigReader {

  private static Logger logger = LoggerFactory.getLogger(ActivityConfigReader.class);

  private static final String ACTIVITY_CONFIG_FILE = "activityConfig.json";

  private static Map<String, SearchServiceUtil> activityServiceConfigMap = new HashMap<>();
  private static Map<SearchServiceUtil, List<String>> serviceTypeFieldsConfigMap = new HashMap<>();

  public static void initialize() {
    InputStream in =
        ActivityConfigReader.class.getClassLoader().getResourceAsStream(ACTIVITY_CONFIG_FILE);

    try {
      loadActivityConfigMap(in);
    } catch (IOException | InstantiationException e) {
      logger.error("File does not exist" + e);
    } catch (ClassNotFoundException e) {
      logger.error("Service class not configured" + e);
    } catch (IllegalAccessException e) {
      logger.error("Illegal action" + e);
    }
  }

  static void loadActivityConfigMap(InputStream in)
      throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
    if (null != in) {
      JsonParser jsonParser = new JsonParser();
      JsonObject jsonObject = (JsonObject) jsonParser.parse(new InputStreamReader(in, "UTF-8"));
      Map<String, Object> activityConfigMap = new Gson().fromJson(jsonObject, Map.class);
      List<Map<String, Object>> activitiesConfigList =
          (List<Map<String, Object>>) activityConfigMap.get(JsonKey.ACTIVITIES);
      for (Map<String, Object> config : activitiesConfigList) {
        List<String> activityTypeList = (List<String>) config.get(JsonKey.TYPE);
        Class<?> classType = Class.forName((String) config.get("serviceClass"));
        SearchServiceUtil searchServiceUtil = (SearchServiceUtil) classType.newInstance();
        for (String activityType : activityTypeList) {
          activityServiceConfigMap.put(activityType, searchServiceUtil);
        }
        serviceTypeFieldsConfigMap.put(
            searchServiceUtil, (List<String>) config.get(JsonKey.FIELDS));
      }
    } else {
      throw new IOException("Config file does not exist");
    }
  }

  public static SearchServiceUtil getServiceUtilClassName(String activityType) {
    return activityServiceConfigMap.get(activityType);
  }

  public static List<String> getFieldsLists(SearchServiceUtil objectType) {
    return serviceTypeFieldsConfigMap.get(objectType);
  }
}
