package validators;

import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.exception.BaseException;
import org.sunbird.exception.ValidationException;
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
    // validate value of status and role of members and userId if provided in request
    validateRoleAndStatus(request);
    return true;
  }

  /**
   * validates UserId, role, status
   *
   * @param request
   * @throws BaseException
   */
  private void validateRoleAndStatus(Request request) throws BaseException {
    Map<String, List<String>> paramValue = new HashMap<>();
    paramValue.put(JsonKey.STATUS, Lists.newArrayList(JsonKey.ACTIVE, JsonKey.INACTIVE));
    paramValue.put(JsonKey.ROLE, Lists.newArrayList(JsonKey.ADMIN, JsonKey.MEMBER));
    List<Map<String, Object>> memberList =
        (List<Map<String, Object>>) request.getRequest().get(JsonKey.MEMBERS);
    if (CollectionUtils.isNotEmpty(memberList)) {
      for (Map<String, Object> member : memberList) {
        if (StringUtils.isBlank((String) member.get(JsonKey.USER_ID))) {
          throw new ValidationException.MandatoryParamMissing(
              JsonKey.MEMBERS + "[" + memberList.indexOf(member) + "]." + JsonKey.USER_ID);
        }
        ValidationUtil.validateParamValue(
            member, Lists.newArrayList(JsonKey.STATUS, JsonKey.ROLE), paramValue);
      }
    }
  }
}
