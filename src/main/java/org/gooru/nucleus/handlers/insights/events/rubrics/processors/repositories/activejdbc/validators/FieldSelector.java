package org.gooru.nucleus.handlers.insights.events.rubrics.processors.repositories.activejdbc.validators;

import java.util.Set;

/**
 * Created by ashish on 28/1/16.
 */
public interface FieldSelector {
    Set<String> allowedFields();

    default Set<String> mandatoryFields() {
        return null;
    }
}
