package validators;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.exception.BaseException;
import org.sunbird.exception.ValidationException;
import org.sunbird.request.Request;
import org.sunbird.util.JsonKey;

public class GroupUpdateRequestValidator implements IRequestValidator {

  private static Logger logger = LoggerFactory.getLogger(GroupUpdateRequestValidator.class);

  @Override
  public boolean validate(Request request) throws BaseException {
    logger.info("Validating the update group request {}", request.getRequest());
    ValidationUtil.validateRequestObject(request);
    ValidationUtil.validateMandatoryParamsWithType(
        request.getRequest(),
        Lists.newArrayList(JsonKey.GROUP_ID),
        String.class,
        true,
        JsonKey.REQUEST);
    validateActivityList(request);
    return true;
  }

  /**
   * checks mandatory param id and type of activity
   *
   * @param request
   * @throws BaseException
   */
  private void validateActivityList(Request request) throws BaseException {
    if (request.getRequest().containsKey(JsonKey.ACTIVITIES)) {
      checkDataTypeOfParam(
          request.getRequest().get(JsonKey.ACTIVITIES),
          JsonKey.ACTIVITIES,
          JsonKey.REQUEST,
          Map.class);
      Map<String, Object> activityList =
          (Map<String, Object>) request.getRequest().get(JsonKey.ACTIVITIES);
      if (MapUtils.isNotEmpty(activityList)) {
        // validate add activity list
        if (activityList.containsKey(JsonKey.ADD)) {
          checkDataTypeOfParam(
              activityList.get(JsonKey.ADD),
              JsonKey.ADD,
              JsonKey.REQUEST + "." + JsonKey.ACTIVITIES,
              List.class);
          List<Map<String, Object>> addActivity =
              (List<Map<String, Object>>) activityList.get(JsonKey.ADD);
          if (CollectionUtils.isNotEmpty(addActivity)) {
            for (Map<String, Object> activity : addActivity) {
              ValidationUtil.validateMandatoryParamsWithType(
                  activity,
                  Lists.newArrayList(JsonKey.ID, JsonKey.TYPE),
                  String.class,
                  true,
                  JsonKey.REQUEST
                      + "."
                      + JsonKey.ACTIVITIES
                      + "."
                      + JsonKey.ADD
                      + "["
                      + addActivity.indexOf(activity)
                      + "]");
            }
          }
        }
        // validate remove list
        if (activityList.containsKey(JsonKey.REMOVE)) {
          checkDataTypeOfParam(
              activityList.get(JsonKey.REMOVE),
              JsonKey.REMOVE,
              JsonKey.REQUEST + "." + JsonKey.ACTIVITIES,
              List.class);
        }
      }
    }
  }

  private void checkDataTypeOfParam(Object object, String key, String parentKey, Class type) {
    if (ObjectUtils.isNotEmpty(object) && !(ValidationUtil.isInstanceOf(object.getClass(), type))) {
      logger.error("validateMandatoryParamsOfStringType:incorrect request provided");
      throw new ValidationException.ParamDataTypeError(parentKey + "." + key, type.getName());
    }
  }
}
