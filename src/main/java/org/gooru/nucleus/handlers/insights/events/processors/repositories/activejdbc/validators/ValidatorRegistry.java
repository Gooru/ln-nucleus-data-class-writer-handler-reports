package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.validators;

/**
 * Created by ashish on 28/1/16.
 */
public interface ValidatorRegistry {

  FieldValidator lookupValidator(String fieldName);

  default FieldValidator noopSuccessValidator(String fieldName) {
    return (n) -> true;
  }

  default FieldValidator noopFailedValidator(String fieldName) {
    return (n) -> false;
  }
}
