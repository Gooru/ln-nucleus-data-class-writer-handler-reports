package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.events.EventParser;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author daniel
 */
class ClassAuthorizedUsersBuilderHandler implements DBHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(ClassAuthorizedUsersBuilderHandler.class);
    private final ProcessorContext context;
    private EventParser event;
    private AJEntityClassAuthorizedUsers classAuthorizers;

    public ClassAuthorizedUsersBuilderHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("invalid request received");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid data received to process events"),
                ExecutionStatus.FAILED);
        }

        LOGGER.debug("checkSanity() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    public ExecutionResult<MessageResponse> validateRequest() {
    	LOGGER.debug("validateRequest() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public ExecutionResult<MessageResponse> executeRequest() {
      event = context.getEvent();
      List<Map> user = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_AUTHORIZED_USER_EXISIST, event.getContentGooruId(), event.getGooruUUID());
      if (user.isEmpty()) {
        LOGGER.debug("classId : {} - userId : {}", event.getContentGooruId(), event.getGooruUUID());
        Base.exec(AJEntityClassAuthorizedUsers.INSERT_AUTHORIZED_USER, event.getContentGooruId(), event.getGooruUUID());
        LOGGER.debug("Class authorized data inserted successfully...");
        return new ExecutionResult<>(MessageResponseFactory.createCreatedResponse(), ExecutionStatus.SUCCESSFUL);
      }else{
        LOGGER.debug("User already present. Do nothing...");
        return new ExecutionResult<>(MessageResponseFactory.createCreatedResponse(), ExecutionStatus.SUCCESSFUL);
      }
    }
    
    @Override
    public boolean handlerReadOnly() {
        return false;
    }
    
}
