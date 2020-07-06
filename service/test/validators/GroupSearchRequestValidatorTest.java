package validators;

import java.util.HashMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sunbird.exception.BaseException;
import org.sunbird.request.Request;
import org.sunbird.util.JsonKey;

public class GroupSearchRequestValidatorTest {

  IRequestValidator groupSearchRequestValidator;

  @Before
  public void setup() {
    groupSearchRequestValidator = new GroupSearchRequestValidator();
  }

  @Test
  public void validateRequestReturnTrue() {
    Request request = createRequestObject();
    request.getRequest().put(JsonKey.FILTERS, new HashMap<>());

    boolean isValidated = false;
    try {
      isValidated = groupSearchRequestValidator.validate(request);
    } catch (BaseException ex) {
      Assert.assertTrue(false);
    }
    Assert.assertTrue(isValidated);
  }

  @Test
  public void validateRequestReturnFalse() {
    Request request = createRequestObject();
    request.getRequest().put("userId", "id1");
    boolean isValidated = true;
    try {
      isValidated = groupSearchRequestValidator.validate(request);
    } catch (BaseException ex) {
      isValidated = false;
    }
    Assert.assertFalse(isValidated);
  }

  private Request createRequestObject() {
    Request request = new Request();
    request.setRequest(new HashMap<>());
    return request;
  }
}
