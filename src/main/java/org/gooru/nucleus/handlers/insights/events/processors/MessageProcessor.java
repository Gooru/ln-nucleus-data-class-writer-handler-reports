package org.gooru.nucleus.handlers.insights.events.processors;

import java.util.ResourceBundle;

import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
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
 * Modified by daniel
 */

class MessageProcessor implements Processor {

  private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class);
  private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("messages");
  private JsonObject messageObj = null;
  private Message<Object> message = null;
  private JsonObject request;
  private EventParser event;
  private ProcessorContext context;

  public MessageProcessor(Message<Object> message) {
    this.message = message;
  }

  public MessageProcessor(JsonObject message) {
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

      /*
       * if(!event.isStudent()){ LOGGER.
       * warn("Receiving teacher activity in player. Ignoring teacher activity.."
       * ); return MessageResponseFactory.
       * createInvalidRequestResponse("Don't process teacher activity in player"
       * ); }
       */
      // final String msgOp =
      // message.headers().get(MessageConstants.MSG_HEADER_OP);
      final String eventName = event.getEventName();
      switch (eventName) {
      case EventConstants.COLLECTION_PLAY:
    	  if (!StringUtil.isNullOrEmpty(event.getContentSource()) 
    			  && event.getContentSource().equalsIgnoreCase(EventConstants.DAILY_CLASS_ACTIVITY)){
    		  result = createDCAReport();    		  
    	  } else {
    		  result = createBaseReport();
    		  if(event.getEventType().equalsIgnoreCase(EventConstants.START)){
    		    result = createUserTaxonomySubject();
    		  }
    	  }
        break;
      case EventConstants.COLLECTION_RESOURCE_PLAY:    	  
    	  if (!StringUtil.isNullOrEmpty(event.getContentSource()) 
    			  && event.getContentSource().equalsIgnoreCase(EventConstants.DAILY_CLASS_ACTIVITY)){
    		  result = createDCAReport();
    		  LOGGER.debug("Taxonomy IDs :  " + event.getTaxonomyIds());
  	        if(event.getTaxonomyIds() != null && !event.getTaxonomyIds().isEmpty()){          
  	          result = createDCACompetencyReport();
  	        }
    	  } else {
    		  result = createBaseReport();    		  
  	        LOGGER.debug("Taxonomy IDs :  " + event.getTaxonomyIds());
  	        if(event.getTaxonomyIds() != null && !event.getTaxonomyIds().isEmpty()){          
  	          result = createCompetencyReport();
  	        }    		  
    	  }
        break;
      case EventConstants.REACTION_CREATE:
        // TODO: Don't Process this event for now. Reaction
        // collection.resource.play itself.
        // We may need to revisit this in future.
        result = MessageResponseFactory.createOkayResponse();
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
  
  private MessageResponse createUserTaxonomySubject() {
    try {
      return RepoBuilder.buildBaseReportingRepo(context).createUserTaxonomySubject();
    } catch (Throwable t) {
      LOGGER.error("Exception while processing createUserTaxonomySubject", t.getMessage());
      return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
    }
  }
  
  private MessageResponse createCompetencyReport() {
	    try {
	      return RepoBuilder.buildBaseReportingRepo(context).insertCompetencyReportsData();
	    } catch (Throwable t) {
	      LOGGER.error("Exception while inserting data for competency report", t.getMessage());
	      return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
	    }
	  }
  
  
  private MessageResponse createDCAReport() {
	    try {
	      return RepoBuilder.buildBaseReportingRepo(context).insertDCAData();
	    } catch (Throwable t) {
	      LOGGER.error("Exception while processing Collection Play Event Data", t.getMessage());
	      return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
	    }
	  }

  private MessageResponse createDCACompetencyReport() {
	    try {
	      return RepoBuilder.buildBaseReportingRepo(context).insertDCACompetencyData();
	    } catch (Throwable t) {
	      LOGGER.error("Exception while inserting data for competency report", t.getMessage());
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

    if (messageObj != null) {
      request = messageObj;
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
