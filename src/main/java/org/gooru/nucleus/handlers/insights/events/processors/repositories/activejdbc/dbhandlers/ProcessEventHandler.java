package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;

import java.sql.Timestamp;

import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.processors.events.EventParser;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.ProcessEventHandler;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

/**
 * Created by mukul@gooru
 */
class ProcessEventHandler implements DBHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(ProcessEventHandler.class);
    private final ProcessorContext context;
    private AJEntityReporting baseReport;
    private EventParser event;

    public ProcessEventHandler(ProcessorContext context) {
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
    public ExecutionResult<MessageResponse> executeRequest() {
    	
    	baseReport = new AJEntityReporting();    	
    	event = context.getEvent();    	
    	
    	baseReport.set("eventName", event.getEventName());
    	baseReport.set("eventType", event.getEventType());
    	baseReport.set("actorId", event.getGooruUUID());
    	baseReport.set("classId", event.getClassGooruId());    	    	
    	baseReport.set("courseId", event.getCourseGooruId());
    	baseReport.set("unitId", event.getUnitGooruId());
    	baseReport.set("lessonId", event.getLessonGooruId());
    	baseReport.set("sessionId", event.getSessionId());    	    	
    	baseReport.set("collectionType", event.getCollectionType());
    	baseReport.set("questionType", event.getQuestionType());
    	baseReport.set("reaction", event.getReaction());
    	baseReport.set("score", event.getScore());    	
    	baseReport.setResourceAttemptStatus(event.getAnswerStatus());    	    	    	

    	if ((event.getEventName().equals(EventConstants.COLLECTION_PLAY)) && (event.getEventType().equals(EventConstants.START))){
    		baseReport.set("collectionId", event.getContentGooruId());
    		baseReport.set("question_count", event.getQuestionCount());
    		baseReport.set("collectionViews", event.getCollectionViews());
    	}

    	if ((event.getEventName().equals(EventConstants.COLLECTION_PLAY)) && (event.getEventType().equals(EventConstants.STOP))){
    		baseReport.set("collectionId", event.getContentGooruId());
    		baseReport.set("collectionTimeSpent", event.getCollectionTimespent());
    	} 

    	
    	if ((event.getEventName().equals(EventConstants.COLLECTION_RESOURCE_PLAY) ) && (event.getEventType().equals(EventConstants.START))){    		
    		baseReport.set("collectionId", event.getParentGooruId());
    		baseReport.set("resourceId", event.getContentGooruId());
    		baseReport.set("resourceViews", event.getResourceViews());
    	}
    	
    	    	
    	if ((event.getEventName().equals(EventConstants.COLLECTION_RESOURCE_PLAY)) && (event.getEventType().equals(EventConstants.STOP))) {
    		baseReport.set("collectionId", event.getParentGooruId());
    		baseReport.set("resourceId", event.getContentGooruId());    		
    		baseReport.set("resourceTimeSpent", event.getResourceTimespent());
    		baseReport.setAnswerObject(event.getAnswerObject().toString());
    	}
    	
    	//Mukul - SetTimeStamp
    	baseReport.set("createTimestamp", new Timestamp(System.currentTimeMillis()));
    	baseReport.set("updateTimestamp", new Timestamp(System.currentTimeMillis())); 
    	
    	Object maxSequenceId =
                Base.firstCell(AJEntityReporting.SELECT_BASEREPORT_MAX_SEQUENCE_ID);
            int sequenceId = 1;
            if (maxSequenceId != null) {
                sequenceId = Integer.valueOf(maxSequenceId.toString()) + 1;
            }
            baseReport.set(AJEntityReporting.SEQUENCE_ID, sequenceId);

    	if (baseReport.hasErrors()) {
            LOGGER.warn("errors in creating Base Report");            
        }

    	LOGGER.info("Actually Inserting Records in DB");
        if (baseReport.isValid()) {
            if (baseReport.insert()) {
                LOGGER.info("Record inserted successfully : " + context.request().toString());  
                return new ExecutionResult<>(MessageResponseFactory.createCreatedResponse(),ExecutionStatus.SUCCESSFUL);
            } else {
                LOGGER.error("Error while inserting event: " + context.request().toString());
                return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
                        ExecutionStatus.FAILED);
            }
        } else {
            LOGGER.warn("Event validation error");
            return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
                    ExecutionStatus.FAILED);
        }
    }

    @Override
    public boolean handlerReadOnly() {
        return false;
    }
}
