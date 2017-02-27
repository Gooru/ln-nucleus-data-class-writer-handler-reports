package org.gooru.nucleus.handlers.insights.events.processors;

import java.util.ResourceBundle;

import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
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
 * Modified by daniel
 */

class MessageProcessor implements Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class);
    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("messages");
    private Message<Object> message = null;  
    private String messageObj = null;   
    private JsonObject request;
    private EventParser event;
    private ProcessorContext context;   

    public MessageProcessor(Message<Object> message) {
        this.message = message;
    }
    
    public MessageProcessor(String message) {
      this.messageObj = message;
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
  
        /*if(!event.isStudent()){
          LOGGER.warn("Receiving teacher activity in player. Ignoring teacher activity..");
          return MessageResponseFactory.createInvalidRequestResponse("Don't process teacher activity in player");
        }*/
        // final String msgOp =
        // message.headers().get(MessageConstants.MSG_HEADER_OP);
        final String eventName = event.getEventName();
        switch (eventName) {
        case EventConstants.COLLECTION_PLAY:
          result = createBaseReport();
          break;
        case EventConstants.COLLECTION_RESOURCE_PLAY:
          LOGGER.debug("Taxonomy IDs :  "+ event.getTaxonomyIds());
          result = createBaseReport();
          if (!event.getTaxonomyIds().isEmpty()) {
            result = createTaxonomyReport();
          }
          break;
        case EventConstants.REACTION_CREATE:
         //TODO: Don't Process this event for now. Reaction collection.resource.play itself.
         //We may need to revist this in future.
          result = MessageResponseFactory.createOkayResponse();
          break;
        case EventConstants.ITEM_DELETE:
          result = reComputeUsageData();
          break;
        case EventConstants.ITEM_CREATE:
          result =  buildClassAuthorizedUser();
          break;
        default:
          LOGGER.error("Invalid operation type passed in, not able to handle");
          return MessageResponseFactory.createInvalidRequestResponse(RESOURCE_BUNDLE.getString("invalid.operation"));
        }
        return result;
      } catch (Throwable e) {
            LOGGER.error("Unhandled exception in processing", e);
            return MessageResponseFactory.createInternalErrorResponse();
        }
    }

    private MessageResponse createBaseReport() {
        try {
            return RepoBuilder.buildBaseReportingRepo(context).insertBaseReportData();            		            
        } catch (Throwable t) {
            LOGGER.error("Exception while processing Collection Play Event Data", t.getMessage());
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }
    }
    
    private MessageResponse reComputeUsageData() {
      try {
          return RepoBuilder.buildBaseReportingRepo(context).reComputeUsageData();                        
      } catch (Throwable t) {
          LOGGER.error("Exception while recomputation", t.getMessage());
          return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
      }
    }
   
    private MessageResponse createTaxonomyReport() {
      try {
          return RepoBuilder.buildBaseReportingRepo(context).insertTaxonomyReportData();                 
      } catch (Throwable t) {
          LOGGER.error("Exception while create taxonomy report", t.getMessage());
          return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
      }
    }

    private MessageResponse buildClassAuthorizedUser() {
      try {
        if (event.getContentFormat().equalsIgnoreCase(EventConstants.CLASS)) {
          return RepoBuilder.buildBaseReportingRepo(context).buildClassAuthorizedUser();
        } else {
          LOGGER.warn("Don't process now. Will revisit later");
          return MessageResponseFactory.createOkayResponse();
        }
      } catch (Throwable t) {
        LOGGER.error("Exception while build class authorizer table", t.getMessage());
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
      if ((messageObj == null && message == null) || (message != null && !(message.body() instanceof JsonObject))
              || (messageObj != null && !(new JsonObject(messageObj) instanceof JsonObject))) {
        LOGGER.error("Invalid message received, either null or body of message is not JsonObject ");
        return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(RESOURCE_BUNDLE.getString("invalid.message")),
                ExecutionResult.ExecutionStatus.FAILED);
      }
      if (message != null) {
        request = ((JsonObject) message.body()).getJsonObject(MessageConstants.MSG_HTTP_BODY);
      }
      if (messageObj != null) {
        request = (new JsonObject(messageObj));
      }
      if (request == null) {
        LOGGER.error("Invalid JSON payload on Message Bus");
        return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(RESOURCE_BUNDLE.getString("invalid.payload")),
                ExecutionResult.ExecutionStatus.FAILED);
      }  
      // All is well, continue processing
      return new ExecutionResult<>(null, ExecutionResult.ExecutionStatus.CONTINUE_PROCESSING);
    }
}
