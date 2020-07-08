package validators;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.exception.BaseException;
import org.sunbird.request.Request;
import org.sunbird.util.JsonKey;

public class GroupUpdateRequestValidator implements IRequestValidator {

  private static Logger logger = LoggerFactory.getLogger(GroupUpdateRequestValidator.class);

  @Override
  public boolean validate(Request request) throws BaseException {
    logger.info(
        "GroupUpdateRequestValidator:started validating the request with request"
            + request.getRequest());
    ValidationUtil.validateRequestObject(request);
    ValidationUtil.validateMandatoryParamsWithType(
        request.getRequest(),
        Lists.newArrayList(JsonKey.GROUP_ID),
        String.class,
        true,
        JsonKey.REQUEST);
    return true;
  }
}
