package org.gooru.nucleus.handlers.insights.events.processors;

import java.util.ResourceBundle;
import java.util.UUID;

import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.gooru.nucleus.handlers.insights.events.processors.events.EventParser;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.RepoBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

/**
 * Created by mukul@gooru
 */

class MessageProcessor implements Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class);
    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("messages");
    private final Message<Object> message;    
    private String userId;
    private JsonObject request;
    private EventParser event;

    public MessageProcessor(Message<Object> message) {
        this.message = message;
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
          
            final String msgOp = message.headers().get(MessageConstants.MSG_HEADER_OP);           
            switch (msgOp) {
            case MessageConstants.MSG_OP_PROCESS_PLAY_EVENTS:
                result = processPlayerEvents();
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

    private MessageResponse processPlayerEvents() {
        try {
            ProcessorContext context = createContext();   
            return RepoBuilder.buildBaseReportingRepo(context).insertPlayerEvents();            		            
        } catch (Throwable t) {
            LOGGER.error("Exception while processing Collection Play Event Data", t.getMessage());
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }
    }
    
   
    private ProcessorContext createContext() {
    	try {    		
			event = new EventParser(request.toString());			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOGGER.error("Error parsing event");
			e.printStackTrace();
		}

        return new ProcessorContext(request, event);        
    }

    private ExecutionResult<MessageResponse> validateAndInitialize() {    	
        if (message == null || !(message.body() instanceof JsonObject)) {
            LOGGER.error("Invalid message received, either null or body of message is not JsonObject ");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse(RESOURCE_BUNDLE.getString("invalid.message")),
                ExecutionResult.ExecutionStatus.FAILED);
        }

        request = ((JsonObject) message.body()).getJsonObject(MessageConstants.MSG_HTTP_BODY);       

                if (request == null) {
            LOGGER.error("Invalid JSON payload on Message Bus");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse(RESOURCE_BUNDLE.getString("invalid.payload")),
                ExecutionResult.ExecutionStatus.FAILED);
        }

        // All is well, continue processing
        return new ExecutionResult<>(null, ExecutionResult.ExecutionStatus.CONTINUE_PROCESSING);
    }

    private boolean validateContext(ProcessorContext context) {
        // No Validation at this point
        return true;
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
