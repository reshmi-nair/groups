package validators;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.exception.BaseException;
import org.sunbird.exception.ValidationException;
import org.sunbird.request.Request;

public class ValidationUtil {

  private static Logger logger = LoggerFactory.getLogger(ValidationUtil.class);

  public static void validateRequestObject(Request request) throws BaseException {
    if (request.getRequest().isEmpty()) {
      logger.error("validateMandatoryParamsOfStringType:incorrect request provided");
      throw new ValidationException.InvalidRequestData();
    }
  }

  public static void validateMandatoryParamsWithType(
      Request request, List<String> mandatoryParamsList, Class<?> type, boolean validatePresence)
      throws BaseException {
    Map<String, Object> reqMap = request.getRequest();

    for (String param : mandatoryParamsList) {
      if (!reqMap.containsKey(param)) {
        throw new ValidationException.MandatoryParamMissing(param);
      }

      if (!(isInstanceOf(reqMap.get(param).getClass(), type))) {
        logger.error("validateMandatoryParamsOfStringType:incorrect request provided");
        throw new ValidationException.ParamDataTypeError(param, type.getName());
      }

      if (validatePresence) {
        validatePresence(param, reqMap.get(param), type);
      }
    }
  }

  private static void validatePresence(String key, Object value, Class<?> type)
      throws BaseException {
    if (type == String.class) {
      if (StringUtils.isBlank((String) value)) {
        logger.error("validatePresence:incorrect request provided");
        throw new ValidationException.MandatoryParamMissing(key);
      }
    } else if (type == Map.class) {
      Map<String, Object> map = (Map<String, Object>) value;
      if (map.isEmpty()) {
        logger.error("validatePresence:incorrect request provided");
        throw new ValidationException.MandatoryParamMissing(key);
      }
    } else if (type == List.class) {
      List<?> list = (List<?>) value;
      if (list.isEmpty()) {
        logger.error("validatePresence:incorrect request provided");
        throw new ValidationException.MandatoryParamMissing(key);
      }
    }
  }

  private static boolean isInstanceOf(Class objClass, Class targetClass) {
    return targetClass.isAssignableFrom(objClass);
  }

  /**
   * @param reqMap
   * @param params list of params to validate values it contains
   * @param paramsValue for each params provided , add a values in the map key should be the
   *     paramName , value should be list of paramValue it should be for example key=status
   *     value=[active, inactive]
   * @throws BaseException
   */
  public static void validateParamValue(
      Map<String, Object> reqMap, List<String> params, Map<String, List<String>> paramsValue)
      throws BaseException {
    logger.info("validating Param Value for the params {} values {}", params, paramsValue);
    for (String param : params) {
      logger.info(" paramsValue.get(param) {}", paramsValue.get(param));
      if (reqMap.containsKey(param) && StringUtils.isNotEmpty((String) reqMap.get(param))) {
        List<String> values = paramsValue.get(param);
        String paramValue = (String) reqMap.get(param);
        if (!values.contains(paramValue)) {
          throw new ValidationException.InvalidParamValue(paramValue, param);
        }
      }
    }
  }
}
