package org.sunbird.util;

import java.util.Arrays;
import java.util.List;

/** This interface will contains all the constants that's used throughout this application. */
public interface JsonKey {

  String ID = "id";
  String MESSAGE = "message";
  String METHOD = "method";
  String REQUEST_MESSAGE_ID = "msgId";
  String VER = "ver";
  String OK = "ok";
  String LOG_LEVEL = "logLevel";
  String ERROR = "error";
  String EMPTY_STRING = "";
  String RESPONSE = "response";
  String KEY = "key";
  String SUCCESS = "success";
  String API_VERSION = "v1";
  String USER_DB = "user_db";
  String SUNBIRD_CASSANDRA_IP = "sunbird_cassandra_host";
  String GROUP_ID = "groupId";
  String GROUP_DESC = "description";
  String GROUP_NAME = "name";
  String MEMBERS = "members";
  String MEMBER = "member";
  String USER_ID = "userId";
  String ROLE = "role";
  String ADMIN = "admin";
  String ACTIVE = "active";
  String INACTIVE = "inactive";
  String GROUP_MEMBERSHIP_TYPE = "membershipType";
  String GROUP_STATUS = "status";
  String URL = "url";
  String LOG_TYPE = "logType";
  String DURATION = "duration";
  String STATUS = "status";
  String INFO = "info";
  String CONTEXT = "context";
  String TELEMETRY_EVENT_TYPE = "telemetryEventType";
  String PARAMS = "params";
  String API_ACCESS = "api_access";
  String FILTERS = "filters";
  String GROUP = "group";
  String UNAUTHORIZED = "Unauthorized";
  String MANAGED_FOR = "managedFor";
  String SSO_REALM = "sso.realm";
  String SSO_URL = "sso.url";
  String ANONYMOUS = "Anonymous";
  List<String> USER_UNAUTH_STATES = Arrays.asList(JsonKey.UNAUTHORIZED, JsonKey.ANONYMOUS);
  String REQUEST = "request";
  String PARENT_ID = "parentId";
  String SUB = "sub";
  String DOT_SEPARATOR = ".";
  String SHA_256_WITH_RSA = "SHA256withRSA";
  String REQUEST_ID = "requestId";
  String VERSION_2 = "v2";
  String ACCESS_TOKEN_PUBLICKEY_BASEPATH = "accesstoken.publickey.basepath";
  String ADD = "add";
  String EDIT = "edit";
  String REMOVE = "remove";
  String INVITE_ONLY = "invite_only";
  String ACTIVITIES = "activities";
  String TYPE = "type";
  String CREATED_ON = "createdOn";
  String UPDATED_ON = "updatedOn";
  String USER_SERVICE_BASE_URL = "LEARNER_SERVICE_PORT";
  String USER_SERVICE_SEARCH_URL = "sunbird_user_service_search_url";
  String SUNBIRD_CS_BASE_URL = "CONTENT_SERVICE_PORT";
  String SUNBIRD_CS_SEARCH_URL = "sunbird_cs_search_url";
  String CONTENT = "content";
  String RESULT = "result";
  String SUNBIRD_HEALTH_CHECK_ENABLE = "sunbird_health_check_enable";
  String HEALTH = "health";
  String PRIVATE = "private";
  String BEARER = "Bearer ";
  String SUNBIRD_CS_AUTH_KEY = "sunbird_cs_token_key";
  String FIELDS = "fields";
  String AUTHORIZATION = "Authorization";
  String ACTIVITY_INFO = "activityInfo";
  String IDENTIFIER = "identifier";
  String FIRSTNAME = "firstName";
  String LASTNAME = "lastName";
  String USER_SERVICE_SYSTEM_SETTING_URL = "sunbird_us_system_setting_url";
  String USER_SERVICE_ORG_READ_URL = "sunbird_us_org_read_url";
  String ENV = "env";
  String REQUEST_TYPE = "requestType";
  Object API_CALL = "API_CALL";
  String USER = "user";
  String ACTOR_TYPE = "actorType";
  String APP_ID = "appId";
  String DEVICE_ID = "deviceId";
  String ACTOR_ID = "actorId";
  String DEFAULT_CONSUMER_ID = "internal";
  String CONSUMER = "consumer";
  String UPDATE = "update";
  String CREATE = "create";
  String DELETE = "delete";
  String REQUEST_SOURCE = "source";
  String CHANNEL = "channel";
  String FIELD = "field";
  String VALUE = "value";
  String CUSTODIAN_ORG_ID = "custodianOrgId";
  String ORGANISATION_ID = "organisationId";
  String HASH_TAG_ID = "hashTagId";
  String MAX_GROUP_MEMBERS_LIMIT = "max_group_members_limit";
  String MAX_ACTIVITY_LIMIT = "max_activity_limit";
  String COUNT = "count";
  String ENABLE_USERID_REDIS_CACHE = "enable_userid_redis_cache";
  String TTL = "ttl";
  String GROUPS_REDIS_TTL = "groups_redis_ttl";
  String USER_REDIS_TTL = "user_redis_ttl";
  String X_REQUEST_ID = "X-Request-ID";
  String ERROR_CODE = "errorCode";
  String MEMBER_EXISTS = "MEMBER_EXISTS";
  String MEMBER_NOT_FOUND = "MEMBER_NOT_FOUND";
  String MAX_GROUP_LIMIT = "max_group_limit";
  String START_TIME = "startTime";
}
