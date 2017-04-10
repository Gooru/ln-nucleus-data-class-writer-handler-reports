package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.events.EventParser;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDCATaxonomyReport;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;

import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DailyClassActivityEventHandler implements DBHandler {
	

	private static final Logger LOGGER = LoggerFactory.getLogger(DailyClassActivityEventHandler.class);
    private final ProcessorContext context;
    private AJEntityDailyClassActivity dcaReport;
    private EventParser event;

    public DailyClassActivityEventHandler(ProcessorContext context) {
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
    	dcaReport = new AJEntityDailyClassActivity();    	
    	event = context.getEvent();    	
        LazyList<AJEntityDailyClassActivity> duplicateRow = null;
    	dcaReport.set("event_name", event.getEventName());
    	dcaReport.set("event_type", event.getEventType());
    	dcaReport.set("actor_id", event.getGooruUUID());
    	dcaReport.set("class_id", event.getClassGooruId());    	    	
    	dcaReport.set("course_id", event.getCourseGooruId());
    	dcaReport.set("unit_id", event.getUnitGooruId());
    	dcaReport.set("lesson_id", event.getLessonGooruId());
    	dcaReport.set("session_id", event.getSessionId());    	    	
    	dcaReport.set("collection_type", event.getCollectionType());
    	dcaReport.set("question_type", event.getQuestionType());
        dcaReport.set("resource_type", event.getResourceType());
    	dcaReport.set("reaction", event.getReaction());
    	dcaReport.set("score", event.getScore());    	
    	dcaReport.setResourceAttemptStatus(event.getAnswerStatus());    	    	    	
    	dcaReport.set("views", event.getViews());
    	dcaReport.set("time_spent", event.getTimespent());
    	dcaReport.set("tenant_id",event.getTenantId());
        dcaReport.set("created_at",new Timestamp(event.getStartTime()));
        dcaReport.set("updated_at",new Timestamp(event.getEndTime()));

    	if ((event.getEventName().equals(EventConstants.COLLECTION_PLAY))){
    	  duplicateRow =  AJEntityDailyClassActivity.findBySQL(AJEntityDailyClassActivity.FIND_COLLECTION_EVENT,event.getSessionId(),event.getContentGooruId(),event.getEventType(), event.getEventName());
    	  dcaReport.set("collection_id", event.getContentGooruId());
    		dcaReport.set("question_count", event.getQuestionCount());
        if (event.getEventType().equalsIgnoreCase(EventConstants.STOP)) {
          Object scoreObj = Base.firstCell(AJEntityDailyClassActivity.COMPUTE_ASSESSMENT_SCORE, event.getSessionId());
          dcaReport.set("score", (double) ((scoreObj != null ? Double.valueOf(scoreObj.toString()) : 0 )* 100) / event.getQuestionCount());
        }
    	}
    	    	
    	if ((event.getEventName().equals(EventConstants.COLLECTION_RESOURCE_PLAY))) {
    	  duplicateRow = AJEntityDailyClassActivity.findBySQL(AJEntityDailyClassActivity.FIND_RESOURCE_EVENT,event.getSessionId(),event.getContentGooruId(),event.getEventType());
    		dcaReport.set("collection_id", event.getParentGooruId());
    		dcaReport.set("resource_id", event.getContentGooruId());    		
    		dcaReport.set("answer_object", event.getAnswerObject().toString());
    	}

    	//Object maxSequenceId =
          //      Base.firstCell(AJEntityDailyClassActivity.SELECT_BASEREPORT_MAX_SEQUENCE_ID);
            //int sequenceId = 1;
            //if (maxSequenceId != null) {
              //  sequenceId = Integer.valueOf(maxSequenceId.toString()) + 1;
            //}
            //baseReport.set(AJEntityReporting.SEQUENCE_ID, sequenceId);

    	if (dcaReport.hasErrors()) {
            LOGGER.warn("Errors in creating DCA Report");            
        }
    	LOGGER.info("Event, Before inserting into DCA Table: " + context.request().toString());
    	
          if (dcaReport.isValid()) {
            if (duplicateRow.isEmpty()) {
              if (dcaReport.insert()) {
                LOGGER.info("Record inserted successfully");
                return new ExecutionResult<>(MessageResponseFactory.createCreatedResponse(), ExecutionStatus.SUCCESSFUL);
              } else {
                LOGGER.error("Error while inserting event: " + context.request().toString());
                return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(), ExecutionStatus.FAILED);
              }
            } else {
              LOGGER.debug("Found duplicate row. so updating duplicate row.....");
              if ((event.getEventName().equals(EventConstants.COLLECTION_RESOURCE_PLAY))) {
                duplicateRow.forEach(dup -> {
                  int id = Integer.valueOf(dup.get("id").toString());
                  long view = (Long.valueOf(dup.get("views").toString()) + event.getViews());
                  long ts = (Long.valueOf(dup.get("time_spent").toString()) + event.getTimespent());
                  long react = event.getReaction() != 0 ? event.getReaction() : 0;
                  Object attmptStatus = dup.get(AJEntityDailyClassActivity.RESOURCE_ATTEMPT_STATUS);
                  Object ansObj = dup.get(AJEntityDailyClassActivity.ANSWER_OBJECT);
                  Base.exec(AJEntityDailyClassActivity.UPDATE_RESOURCE_EVENT, view, ts, event.getScore(), react, attmptStatus, ansObj, id);
                });
      
              }
              if ((event.getEventName().equals(EventConstants.COLLECTION_PLAY))) {
                duplicateRow.forEach(dup -> {
                  int id = Integer.valueOf(dup.get("id").toString());
                  long view = (Long.valueOf(dup.get("views").toString()) + event.getViews());
                  long ts = (Long.valueOf(dup.get("time_spent").toString()) + event.getTimespent());
                  long react = event.getReaction() != 0 ? event.getReaction() : 0;
                  Base.exec(AJEntityDailyClassActivity.UPDATE_COLLECTION_EVENT, view, ts, event.getScore(), react,id);
                });
              }
            }
          } else {
            LOGGER.warn("Event validation error");
            return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
                    ExecutionStatus.FAILED);
        }

        return new ExecutionResult<>(MessageResponseFactory.createCreatedResponse(), ExecutionStatus.SUCCESSFUL);

    }

    @Override
    public boolean handlerReadOnly() {
        return false;
    }
    
}
