package org.gooru.nucleus.handlers.insights.events.processors;

import java.util.ResourceBundle;

import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
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
 * 
 */

public class SelfReportingProcessor implements Processor {

	  private static final Logger LOGGER = LoggerFactory.getLogger(SelfReportingProcessor.class);
	  private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("messages");	  
	  private Message<Object> message = null;
	  private JsonObject request;	  
	  private ProcessorContext context;

	  public SelfReportingProcessor(Message<Object> message) {
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
	      result = updateStudentSelfReportedScore();

	      return result;
	      
	    } catch (Throwable e) {
	      LOGGER.error("Unhandled exception in processing", e);
	      return MessageResponseFactory.createInternalErrorResponse();
	    }
	  }

	  private MessageResponse updateStudentSelfReportedScore() {
	    try {
	      return RepoBuilder.buildBaseReportingRepo(context).updateStudentSelfReportedScore();
	    } catch (Throwable t) {
	      LOGGER.error("Exception while processing Student Self Reporting Score Data ", t.getMessage());
	      return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
	    }
	  }

	  private ProcessorContext createContext() {	    
	    LOGGER.info("context.request at SelfReportingProcessor" + request);
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
