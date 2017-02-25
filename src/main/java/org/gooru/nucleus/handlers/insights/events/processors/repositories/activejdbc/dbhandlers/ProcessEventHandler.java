package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.sql.Timestamp;

import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.events.EventParser;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    @SuppressWarnings("rawtypes")
    public ExecutionResult<MessageResponse> executeRequest() {
    	baseReport = new AJEntityReporting();    	
    	event = context.getEvent();    	
      LazyList<AJEntityReporting> duplicateRow = null;
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
      baseReport.set("resourcetype", event.getResourceType());
    	baseReport.set("reaction", event.getReaction());
    	baseReport.set("score", event.getScore());    	
    	baseReport.setResourceAttemptStatus(event.getAnswerStatus());    	    	    	
    	baseReport.set("views", event.getViews());
    	baseReport.set("timespent", event.getTimespent());
    	baseReport.set("tenantId",event.getTenantId());

    	if ((event.getEventName().equals(EventConstants.COLLECTION_PLAY))){
    	  duplicateRow =  AJEntityReporting.findBySQL(AJEntityReporting.FIND_COLLECTION_EVENT,event.getSessionId(),event.getContentGooruId(),event.getEventType(), event.getEventName());
    	  baseReport.set("collectionId", event.getContentGooruId());
    		baseReport.set("question_count", event.getQuestionCount());
        if (event.getEventType().equalsIgnoreCase(EventConstants.STOP)) {
          Object scoreObj = Base.firstCell(AJEntityReporting.COMPUTE_ASSESSMENT_SCORE, event.getSessionId());
          baseReport.set("score", Math.round((double) ((scoreObj != null ? Integer.valueOf(scoreObj.toString()) : 0 )* 100) / event.getQuestionCount()));
        }
    	}
    	    	
    	if ((event.getEventName().equals(EventConstants.COLLECTION_RESOURCE_PLAY))) {
    	  duplicateRow = AJEntityReporting.findBySQL(AJEntityReporting.FIND_RESOURCE_EVENT,event.getSessionId(),event.getContentGooruId(),event.getEventType());
    		baseReport.set("collectionId", event.getParentGooruId());
    		baseReport.set("resourceId", event.getContentGooruId());    		
    		baseReport.set("answerObject", event.getAnswerObject().toString());
    	}
    	
    	//Mukul - SetTimeStamp
    	baseReport.set("createTimestamp", new Timestamp(event.getStartTime()));
    	baseReport.set("updateTimestamp", new Timestamp(event.getEndTime())); 
    	
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

    	LOGGER.info("Before insert: " + context.request().toString());
    	
          if (baseReport.isValid()) {
            if (duplicateRow.isEmpty()) {
              if (baseReport.insert()) {
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
                  long ts = (Long.valueOf(dup.get("timespent").toString()) + event.getTimespent());
                  long react = event.getReaction() != 0 ? event.getReaction() : 0;
                  Object attmptStatus = dup.get(AJEntityReporting.RESOURCE_ATTEMPT_STATUS);
                  Object ansObj = dup.get(AJEntityReporting.ANSWER_OBJECT);
                  Base.exec(AJEntityReporting.UPDATE_RESOURCE_EVENT, view, ts, event.getScore(), react, attmptStatus, ansObj, id);
                });
      
              }
              if ((event.getEventName().equals(EventConstants.COLLECTION_PLAY))) {
                duplicateRow.forEach(dup -> {
                  int id = Integer.valueOf(dup.get("id").toString());
                  long view = (Long.valueOf(dup.get("views").toString()) + event.getViews());
                  long ts = (Long.valueOf(dup.get("timespent").toString()) + event.getTimespent());
                  long react = event.getReaction() != 0 ? event.getReaction() : 0;
                  Base.exec(AJEntityReporting.UPDATE_COLLECTION_EVENT, view, ts, event.getScore(), react,id);
                });
              }
              return new ExecutionResult<>(MessageResponseFactory.createCreatedResponse(), ExecutionStatus.SUCCESSFUL);
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
