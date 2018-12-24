package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.validators;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.UUID;

/**
 * Created by ashish on 28/1/16.
 */
public interface FieldValidator {

  static boolean validateStringAllowNullOrEmpty(Object o, int len) {
    return o == null || (o instanceof String && (((String) o).isEmpty()
        || ((String) o).length() <= len));
  }

  static boolean validateStringIfPresent(Object o, int len) {
    return o == null || o instanceof String && !((String) o).isEmpty()
        && ((String) o).length() < len;
  }

  static boolean validateString(Object o, int len) {
    return !(o == null || !(o instanceof String) || ((String) o).isEmpty() || (((String) o).length()
        > len));
  }

  static boolean validateJsonIfPresent(Object o) {
    return o == null || o instanceof JsonObject && !((JsonObject) o).isEmpty();
  }

  static boolean validateJson(Object o) {
    return !(o == null || !(o instanceof JsonObject) || ((JsonObject) o).isEmpty());
  }

  static boolean validateJsonArrayIfPresent(Object o) {
    return o == null || o instanceof JsonArray && !((JsonArray) o).isEmpty();
  }

  static boolean validateJsonArray(Object o) {
    return !(o == null || !(o instanceof JsonArray) || ((JsonArray) o).isEmpty());
  }

  static boolean validateDeepJsonArrayIfPresent(Object o, FieldValidator fv) {
    if (o == null) {
      return true;
    } else if (!(o instanceof JsonArray) || ((JsonArray) o).isEmpty()) {
      return false;
    } else {
      JsonArray array = (JsonArray) o;
      for (Object element : array) {
        if (!fv.validateField(element)) {
          return false;
        }
      }
    }
    return true;
  }

  static boolean validateDeepJsonArray(Object o, FieldValidator fv) {
    if (o == null || !(o instanceof JsonArray) || ((JsonArray) o).isEmpty()) {
      return false;
    }
    JsonArray array = (JsonArray) o;
    for (Object element : array) {
      if (!fv.validateField(element)) {
        return false;
      }
    }
    return true;
  }

  static boolean validateBoolean(Object o) {
    return o != null && o instanceof Boolean;
  }

  static boolean validateBooleanIfPresent(Object o) {
    return o == null || o instanceof Boolean;
  }

  static boolean validateIntegerIfPresent(Object o) {
    return o == null || o instanceof Integer;
  }

  static boolean validateDoubleIfPresent(Object o) {
    boolean result = (o == null || o instanceof Double);
    if (!result) {
      result = validateIntegerIfPresent(o);
    }
    return result;
  }

  static boolean validateUuid(Object o) {
    try {
      UUID.fromString((String) o);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  static boolean validateUuidIfPresent(Object o) {
    return o == null || validateUuid(o);
  }

  boolean validateField(Object value);
}
