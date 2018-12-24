package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.validators;

import io.vertx.core.json.JsonObject;
import java.util.ResourceBundle;
import java.util.Set;

/**
 * Created by ashish on 28/1/16.
 */
public interface PayloadValidator {

  ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("messages");

  default JsonObject validatePayload(JsonObject input, FieldSelector selector,
      ValidatorRegistry registry) {
    JsonObject result = new JsonObject();
    input.forEach(entry -> {
      if (selector.allowedFields().contains(entry.getKey())) {
        FieldValidator validator = registry.lookupValidator(entry.getKey());
        if (validator != null) {
          if (!validator.validateField(entry.getValue())) {
            result.put(entry.getKey(), RESOURCE_BUNDLE.getString("invalid.value"));
          }
        }
      } else {
        result.put(entry.getKey(), RESOURCE_BUNDLE.getString("field.not.allowed"));
      }
    });
    Set<String> mandatory = selector.mandatoryFields();
    if (mandatory != null && mandatory.isEmpty()) {
      mandatory.forEach(s -> {
        if (input.getValue(s) == null) {
          result.put(s, RESOURCE_BUNDLE.getString("missing.field"));
        }
      });
    }
    return result.isEmpty() ? null : result;
  }
}
