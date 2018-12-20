package org.gooru.nucleus.handlers.insights.events.constants;

public final class MessageConstants {

  public static final String MSG_HEADER_OP = "mb.operation";
  public static final String MSG_HEADER_TOKEN = "session.token";
  public static final String MSG_OP_AUTH_WITH_PREFS = "auth.with.prefs";
  public static final String MSG_OP_STATUS = "mb.operation.status";
  public static final String MSG_KEY_PREFS = "prefs";
  public static final String MSG_OP_STATUS_SUCCESS = "success";
  public static final String MSG_OP_STATUS_ERROR = "error";
  public static final String MSG_OP_STATUS_VALIDATION_ERROR = "error.validation";
  public static final String MSG_USER_ANONYMOUS = "anonymous";
  public static final String MSG_USER_ID = "user_id";
  public static final String MSG_HTTP_STATUS = "http.status";
  public static final String MSG_HTTP_BODY = "http.body";
  public static final String MSG_HTTP_RESPONSE = "http.response";
  public static final String MSG_HTTP_ERROR = "http.error";
  public static final String MSG_HTTP_VALIDATION_ERROR = "http.validation.error";
  public static final String MSG_HTTP_HEADERS = "http.headers";
  public static final String MSG_MESSAGE = "message";

  // Containers for different responses
  public static final String RESP_CONTAINER_MBUS = "mb.container";
  public static final String RESP_CONTAINER_EVENT = "mb.event";

  //Class Reports - Process Collection/Resource.Play Events
  public static final String MSG_OP_PROCESS_PLAY_EVENTS = "process.play.events";

  //configuration constants
  public static final String COURSE = "course";
  public static final String UNIT = "unit";
  public static final String LESSON = "lesson";
  public static final String COLLECTION = "collection";
  public static final String ASSESSMENT = "assessment";
  public static final String CLASS = "class";

  public static final String SUBJECT = "subject";
  public static final String DOMAIN = "domain";
  public static final String STANDARDS = "standard";
  public static final String MICRO_STANDARDS = "micro_standard";
  public static final String LEARNING_TARGETS = "learning_target";

  public static final String HYPHEN = "-";
  public static final String COMMA = ",";
  public static final String COLON = ":";


  private MessageConstants() {
    throw new AssertionError();
  }
}
