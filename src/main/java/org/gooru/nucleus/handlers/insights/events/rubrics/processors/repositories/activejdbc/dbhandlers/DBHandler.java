package org.gooru.nucleus.handlers.insights.events.rubrics.processors.repositories.activejdbc.dbhandlers;

import org.gooru.nucleus.handlers.insights.events.rubrics.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.rubrics.processors.responses.MessageResponse;


public interface DBHandler {
    ExecutionResult<MessageResponse> checkSanity();

    ExecutionResult<MessageResponse> validateRequest();

    ExecutionResult<MessageResponse> executeRequest();

    boolean handlerReadOnly();
}
