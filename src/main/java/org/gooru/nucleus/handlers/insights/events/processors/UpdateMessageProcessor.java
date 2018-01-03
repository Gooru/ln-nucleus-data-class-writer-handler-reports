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

import io.netty.util.internal.StringUtil;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

/**
 * Created by mukul@gooru 
 * 
 */

public class UpdateMessageProcessor implements Processor {

	  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateMessageProcessor.class);
	  private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("messages");	  
	  private Message<Object> message = null;
	  private JsonObject request;	  
	  private ProcessorContext context;

	  public UpdateMessageProcessor(Message<Object> message) {
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
	      context = createContext();

	      final String eventName = request.getString(EventConstants._EVENT_NAME);
	      LOGGER.debug(eventName);
	      switch (eventName) {	      
	      case EventConstants.COLLECTION_RESOURCE_UPDATE:
	        result = updateAssessmentScore();
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

	  private MessageResponse updateAssessmentScore() {
	    try {
	      return RepoBuilder.buildBaseReportingRepo(context).updateAssessmentScore();
	    } catch (Throwable t) {
	      LOGGER.error("Exception while processing Collection Play Event Data", t.getMessage());
	      return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
	    }
	  }

	  private ProcessorContext createContext() {	    
	    LOGGER.info("context.request at UpdateMessageProcessor" + request);
	    //No UserId, so assigning it to Null. Ideally update the createContext() method to NOT have
	    //userId as the parameter, since even Rubrics doesn't need that anymore.
	      return new ProcessorContext(null, request);  
	  }

	  private ExecutionResult<MessageResponse> validateAndInitialize() {

		 if (message != null) {
	          request = ((JsonObject) message.body()).getJsonObject(MessageConstants.MSG_HTTP_BODY);
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
