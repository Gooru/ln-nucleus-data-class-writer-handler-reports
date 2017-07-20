package org.gooru.nucleus.handlers.insights.events.rubrics.processors;

import java.util.ResourceBundle;

import java.util.UUID;

import org.gooru.nucleus.handlers.insights.events.rubrics.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.rubrics.processors.repositories.RepoBuilder;
import org.gooru.nucleus.handlers.insights.events.rubrics.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.rubrics.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.rubrics.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;


/**
 * Created by mukul@gooru
 * 
 */

class MessageProcessor implements RubricProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProcessor.class);
    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("messages");
    private Message<Object> message = null;
    private String messageObj = null;
    private JsonObject messageJobj = null;
    private String userId;
    private JsonObject prefs;
    private JsonObject request;
    private ProcessorContext context;
    
        
    public MessageProcessor(Message<Object> message) {
        this.message = message;
    }
    
    public MessageProcessor(String message) {
      this.messageObj = message;
  }
    
    public MessageProcessor(JsonObject message) {
        this.messageJobj = message;
      }


    @Override
    public MessageResponse process() {
        MessageResponse result;
      try {
        // Validate the message itself
        ExecutionResult<MessageResponse> validateResult = validateAndInitialize();
        if (validateResult.isCompleted()) {
          return validateResult.result();
        }
        
        context = createContext();
        
        final String eventName = messageJobj.getString(EventConstants._EVENT_NAME);
        LOGGER.debug(eventName);
        switch (eventName) {
        //case MessageConstants.MSG_OP_STUDENTS_GRADES_WRITE:
        case EventConstants.RESOURCE_RUBRIC_GRADE:
        	result = procStudentGrades();
        	break;
        default:
            LOGGER.error("Invalid operation type passed in, not able to handle");
            return MessageResponseFactory
                .createInvalidRequestResponse(RESOURCE_BUNDLE.getString("invalid.operation"));
        }
        return result;
      } catch (Throwable e) {
            LOGGER.error("Unhandled exception in processing", e);
            return MessageResponseFactory.createInternalErrorResponse();
        }
    }
    
    
   

    private MessageResponse procStudentGrades() {
    	try {
            ProcessorContext context = createContext();
            
            return new RepoBuilder().buildRubricGradingRepo(context).processStudentGrades();
            
        } catch (Throwable t) {
            LOGGER.error("Exception while getting Students Grades for a Question", t);
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }

    }


    
    //TODO: This method needs to be updated 
    private ProcessorContext createContext() {
    	
    	LOGGER.info("context.request(1)" + request);
        return new ProcessorContext(userId, request);  
    }

    //This is just the first level validation. Each Individual Handler would need to do more validation based on the
    //handler specific params that are needed for processing.
    private ExecutionResult<MessageResponse> validateAndInitialize() {    	
        if (message != null) {
          request = ((JsonObject) message.body()).getJsonObject(MessageConstants.MSG_HTTP_BODY);
        }
        
        if (messageObj != null) {
          request = (new JsonObject(messageObj));
        }
        
        if (messageJobj != null) {
            request = messageJobj;
        }
        
        if (request == null) {
          LOGGER.error("Invalid JSON payload on Message Bus");
          return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(RESOURCE_BUNDLE.getString("invalid.payload")),
                  ExecutionResult.ExecutionStatus.FAILED);
        }
    
        // All is well, continue processing
        return new ExecutionResult<>(null, ExecutionResult.ExecutionStatus.CONTINUE_PROCESSING);

        }
  
    private boolean validateUser(String userId) {
        return !(userId == null || userId.isEmpty()
            || (userId.equalsIgnoreCase(MessageConstants.MSG_USER_ANONYMOUS)) && validateUuid(userId));
    }

    private boolean validateId(String id) {
        return !(id == null || id.isEmpty()) && validateUuid(id);
    }

    private boolean validateUuid(String uuidString) {
        try {
            UUID uuid = UUID.fromString(uuidString);
            return true;
        } catch (IllegalArgumentException e) {        	
            return false;
        } catch (Exception e) {        	
            return false;
        }
    }

}
