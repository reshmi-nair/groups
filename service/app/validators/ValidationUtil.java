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

  public static void validateMandatoryParamsOfStringType(
      Request request, List<String> mandatoryParamsList) throws BaseException {
    Map<String, Object> reqMap = request.getRequest();

    for (String param : mandatoryParamsList) {
      if (!reqMap.containsKey(param)) {
        throw new ValidationException.MandatoryParamMissing(param);
      }
      if (!(reqMap.get(param) instanceof String)) {
        logger.error("validateMandatoryParamsOfStringType:incorrect request provided");
        throw new ValidationException.ParamDataTypeError(param, "string");
      }
      validatePresence(param, (String) reqMap.get(param));
    }
  }

  private static void validatePresence(String key, String value) throws BaseException {
    if (StringUtils.isBlank(value)) {
      logger.error("validatePresence:incorrect request provided");
      throw new ValidationException.MandatoryParamMissing(key);
    }
  }
}
