package validators;

import com.google.common.collect.Lists;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.exception.BaseException;
import org.sunbird.request.Request;
import org.sunbird.util.JsonKey;

public class GroupSearchRequestValidator implements validators.IRequestValidator {
  private static Logger logger =
      LoggerFactory.getLogger(validators.GroupSearchRequestValidator.class);

  @Override
  public boolean validate(Request request) throws BaseException {
    logger.info(
        "GroupSearchRequestValidator:started validating the request with request"
            + request.getRequest());
    validators.ValidationUtil.validateRequestObject(request);
    validators.ValidationUtil.validateMandatoryParamsWithType(
        request, Lists.newArrayList(JsonKey.FILTERS), Map.class, false);
    return true;
  }
}
