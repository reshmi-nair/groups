package validators;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.exception.BaseException;
import org.sunbird.request.Request;
import org.sunbird.util.JsonKey;

public class GroupCreateRequestValidator implements IRequestValidator {

  private static Logger logger = LoggerFactory.getLogger(GroupCreateRequestValidator.class);

  @Override
  public boolean validate(Request request) throws BaseException {
    logger.info(
        "GroupCreateRequestValidator:started validating the request with request"
            + request.getRequest());
    ValidationUtil.validateRequestObject(request);
    ValidationUtil.validateMandatoryParamsWithType(
        request, Lists.newArrayList(JsonKey.GROUP_NAME), String.class, true);
    return true;
  }
}
