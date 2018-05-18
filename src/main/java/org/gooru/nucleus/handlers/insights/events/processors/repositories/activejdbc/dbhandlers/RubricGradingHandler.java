package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.GEPConstants;
import org.gooru.nucleus.handlers.insights.events.processors.MessageDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityRubricGrading;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.EntityBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;


/**
 * Created by mukul@gooru
 */

public class RubricGradingHandler implements DBHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(RubricGradingHandler.class);
	public static final String TOPIC_GEP_USAGE_EVENTS = "org.gooru.da.sink.logW.usage.events";
	private final ProcessorContext context;
	private AJEntityRubricGrading rubricGrading;
	private Double score;
	private Double max_score;

    public RubricGradingHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
         if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("Invalid Rubric Grading Data");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid Rubric Grading Data"),
                ExecutionStatus.FAILED);
        }

        LOGGER.debug("checkSanity() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    public ExecutionResult<MessageResponse> validateRequest() {
      if (context.request().getString("userIdFromSession") != null) {
        List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, context.request().getString("class_id"),
                context.request().getString("userIdFromSession"));
        if (owner.isEmpty()) {
          LOGGER.warn("User is not a teacher or collaborator");
          // return new
          // ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User
          // is not a teacher/collaborator"), ExecutionStatus.FAILED);
        }
      }
      LOGGER.debug("validateRequest() OK");
      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
      public ExecutionResult<MessageResponse> executeRequest() {
    
    	rubricGrading = new AJEntityRubricGrading();
        LazyList<AJEntityReporting> allGraded = null;
    	JsonObject req = context.request(); 
    	String teacherId = req.getString("userIdFromSession");
    	req.remove("userIdFromSession");
    	//Analytics will not store gut_codes in the Rubric Grading Table.
    	req.remove(AJEntityRubricGrading.GUT_CODES);
    	LazyList<AJEntityReporting> duplicateRow = null;

    	new DefAJEntityRubricGradingEntityBuilder().build(rubricGrading, req, AJEntityRubricGrading.getConverterRegistry());
    	Object collType = Base.firstCell(AJEntityReporting.FIND_COLLECTION_TYPE, rubricGrading.get(AJEntityRubricGrading.CLASS_ID),
    			rubricGrading.get(AJEntityRubricGrading.COURSE_ID), rubricGrading.get(AJEntityRubricGrading.UNIT_ID),
    			rubricGrading.get(AJEntityRubricGrading.LESSON_ID), rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID));
    	LOGGER.debug("Collection Type " + collType);

    	//Currently this situation may arise if the teacher decides to change the Grades in the current Rubric Grading session
    	//using the Prev-Next buttons (ref: UI). So the Rubric associated to a question will always be the same.
    	//However teacher only gets One Attempt at Grading. (i.e once this Rubric Grading session is closed by the teacher, 
    	// the grades cannot be changed)
    	duplicateRow =  AJEntityReporting.findBySQL(AJEntityRubricGrading.THIS_QUESTION_GRADES_FIND, 
    			rubricGrading.get(AJEntityRubricGrading.STUDENT_ID), rubricGrading.get(AJEntityRubricGrading.CLASS_ID),
    			rubricGrading.get(AJEntityRubricGrading.COURSE_ID), rubricGrading.get(AJEntityRubricGrading.UNIT_ID),
    			rubricGrading.get(AJEntityRubricGrading.LESSON_ID), rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID),
    			rubricGrading.get(AJEntityRubricGrading.RESOURCE_ID), rubricGrading.get(AJEntityRubricGrading.SESSION_ID));

    	if (duplicateRow == null || duplicateRow.isEmpty()) {
    		if (collType != null){
    			rubricGrading.set("collection_type", collType.toString());        	
    		}

    		rubricGrading.set(AJEntityRubricGrading.GRADER_ID,teacherId);
    		rubricGrading.set("grader", "Teacher");
    		//Timestamps are mandatory 
    		rubricGrading.set("created_at", new Timestamp(Long.valueOf(rubricGrading.get(AJEntityRubricGrading.CREATED_AT).toString())));
    		rubricGrading.set("updated_at", new Timestamp(Long.valueOf(rubricGrading.get(AJEntityRubricGrading.UPDATED_AT).toString())));

    		boolean result = rubricGrading.save();
    		if (!result) {
    			LOGGER.error("Grades cannot be inserted into the DB for the student " + req.getValue(AJEntityRubricGrading.STUDENT_ID));
    			if (rubricGrading.hasErrors()) {
    				Map<String, String> map = rubricGrading.errors();
    				JsonObject errors = new JsonObject();
    				map.forEach(errors::put);
    				return new ExecutionResult<>(MessageResponseFactory.createValidationErrorResponse(errors), ExecutionResult.ExecutionStatus.FAILED);
    			}
    		}
    		LOGGER.debug("Student Rubric grades stored successfully for the student " + req.getValue(AJEntityRubricGrading.STUDENT_ID));        	
    	} else if (duplicateRow != null) {
    		LOGGER.debug("Found duplicate row in the DB, this question appears to be graded previously, will be updated");
    		duplicateRow.forEach(dup -> {
    			int id = Integer.valueOf(dup.get("id").toString());
    			//update the Answer Object and Answer Status from the latest event
    			Base.exec(AJEntityRubricGrading.UPDATE_QUESTION_GRADES, rubricGrading.get(AJEntityRubricGrading.STUDENT_SCORE),
    					rubricGrading.get(AJEntityRubricGrading.MAX_SCORE), rubricGrading.get(AJEntityRubricGrading.OVERALL_COMMENT),
    					rubricGrading.get(AJEntityRubricGrading.CATEGORY_SCORE), 
    					new Timestamp(Long.valueOf(rubricGrading.get(AJEntityRubricGrading.UPDATED_AT).toString())), id);
    		});
    	}

    	Object sessionId = rubricGrading.get(AJEntityRubricGrading.SESSION_ID);
    	if (sessionId == null) {
    		return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid SessionId"), ExecutionResult.ExecutionStatus.FAILED);
    	}

    	if (sessionId != null) {
    		LOGGER.debug("Student Score : {} ", rubricGrading.get(AJEntityRubricGrading.STUDENT_SCORE));
    		LOGGER.debug("Max Score : {} ", rubricGrading.get(AJEntityRubricGrading.MAX_SCORE));
    		LOGGER.debug("session id : {} ", sessionId.toString());
    		LOGGER.debug("resource id : {} ", rubricGrading.get(AJEntityRubricGrading.RESOURCE_ID));

    		Base.exec(AJEntityReporting.UPDATE_QUESTION_SCORE, rubricGrading.get(AJEntityRubricGrading.STUDENT_SCORE),
    				rubricGrading.get(AJEntityRubricGrading.MAX_SCORE), true, sessionId.toString(), rubricGrading.get(AJEntityRubricGrading.RESOURCE_ID));

    		LOGGER.debug("Computing total score...");
    		LazyList<AJEntityReporting> scoreTS = AJEntityReporting.findBySQL(AJEntityReporting.COMPUTE_ASSESSMENT_SCORE_POST_GRADING, 
    				rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID), sessionId.toString());
    		LOGGER.debug("scoreTS {} ", scoreTS);

    		if (scoreTS != null && !scoreTS.isEmpty()) {
    			scoreTS.forEach(m -> {
    				score = (m.get(AJEntityReporting.SCORE) != null ? Double.valueOf(m.get(AJEntityReporting.SCORE).toString()) : null);
    				LOGGER.debug("score {} ", score);
    				max_score = (m.get(AJEntityReporting.MAX_SCORE) != null ? Double.valueOf(m.get(AJEntityReporting.MAX_SCORE).toString()) : null);
    				LOGGER.debug("max_score {} ", max_score);        
    			});

    			if (score != null && max_score != null && max_score > 0.0) {
    				score = ((score * 100) / max_score);
    				LOGGER.debug("Re-Computed total score {} ", score);
    			}
    		}
    		Base.exec(AJEntityReporting.UPDATE_ASSESSMENT_SCORE, score, max_score, rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID), sessionId.toString());
    		LOGGER.debug("Total score updated successfully...");
    	}

		Object sId = Base.firstCell(AJEntityReporting.FIND_SESSION_ID, rubricGrading.get(AJEntityRubricGrading.STUDENT_ID),
				rubricGrading.get(AJEntityRubricGrading.CLASS_ID),
				rubricGrading.get(AJEntityRubricGrading.COURSE_ID), rubricGrading.get(AJEntityRubricGrading.UNIT_ID),
				rubricGrading.get(AJEntityRubricGrading.LESSON_ID), rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID));
		
		 	
    	if ((sId != null && sId.toString().equals(sessionId.toString())) && (rubricGrading.get(AJEntityRubricGrading.STUDENT_SCORE) != null)) {
    		LOGGER.info("Sending Resource Update Score Event to GEP");
    		sendResourceScoreUpdateEventtoGEP(collType.toString());
    	}
    	 
    	if ((sId != null && sId.toString().equals(sessionId.toString())) && score != null && max_score != null) {
    		LOGGER.debug("Student Id is: " + rubricGrading.get(AJEntityRubricGrading.STUDENT_ID));
    		LOGGER.debug("Latest Session Id is: " + sId.toString());
    		LOGGER.debug("Collection Id is: " + rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID));
    		
    		LOGGER.info("Sending Collection Update Score Event to GEP");
        	  allGraded =  AJEntityReporting.findBySQL(AJEntityReporting.IS_COLLECTION_GRADED, rubricGrading.get(AJEntityRubricGrading.STUDENT_ID), 
        			  sId.toString(), rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID), EventConstants.COLLECTION_RESOURCE_PLAY, EventConstants.STOP, false);
              if (allGraded == null || allGraded.isEmpty()) {
            	  sendCollScoreUpdateEventtoGEP(collType.toString());            	  
              }    		
    	}

    	LOGGER.debug("DONE");
    	return new ExecutionResult<>(MessageResponseFactory.createCreatedResponse(), ExecutionStatus.SUCCESSFUL);
      }   
    
    private static class DefAJEntityRubricGradingEntityBuilder implements EntityBuilder<AJEntityRubricGrading> {
    }
    

    @Override
    public boolean handlerReadOnly() {
        return false;
    }
    
    private void sendResourceScoreUpdateEventtoGEP(String cType) {
    	
    	JsonObject gepEvent = createResourceScoreUpdateEvent(cType);

    	try {
    		LOGGER.debug("The Collection GEP Event is : {} ", gepEvent);
    		MessageDispatcher.getInstance().sendGEPEvent2Kafka(TOPIC_GEP_USAGE_EVENTS, gepEvent);
    		LOGGER.info("Successfully dispatched Update Resource Score GEP Event..");
    	} catch (Exception e) {
    		LOGGER.error("Error while dispatching Update Resource Score GEP Event ", e);
    	}
    }
    
    private JsonObject createResourceScoreUpdateEvent(String collectionType) {    	
    	JsonObject resEvent = new JsonObject();
    	JsonObject context = new JsonObject();
    	JsonObject result = new JsonObject();

    	resEvent.put(GEPConstants.USER_ID, rubricGrading.get(AJEntityRubricGrading.STUDENT_ID));
    	resEvent.put(GEPConstants.ACTIVITY_TIME, System.currentTimeMillis());
    	resEvent.put(GEPConstants.EVENT_ID, UUID.randomUUID().toString());
    	resEvent.put(GEPConstants.EVENT_NAME, GEPConstants.RES_SCORE_UPDATE_EVENT);
    	resEvent.put(GEPConstants.RESOURCE_ID, rubricGrading.get(AJEntityRubricGrading.RESOURCE_ID));
    	resEvent.put(GEPConstants.RESOURCE_TYPE, GEPConstants.QUESTION);

    	context.put(GEPConstants.CLASS_ID, rubricGrading.get(AJEntityRubricGrading.CLASS_ID));
    	context.put(GEPConstants.COURSE_ID, rubricGrading.get(AJEntityRubricGrading.COURSE_ID));
    	context.put(GEPConstants.UNIT_ID, rubricGrading.get(AJEntityRubricGrading.UNIT_ID));
    	context.put(GEPConstants.LESSON_ID, rubricGrading.get(AJEntityRubricGrading.LESSON_ID));
    	context.put(GEPConstants.COLLECTION_ID, rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID));
    	context.put(GEPConstants.COLLECTION_TYPE, collectionType);
    	context.put(GEPConstants.SESSION_ID, rubricGrading.get(AJEntityRubricGrading.SESSION_ID));

    	resEvent.put(GEPConstants.CONTEXT, context);

    	result.put(GEPConstants.SCORE, rubricGrading.get(AJEntityRubricGrading.STUDENT_SCORE));
    	result.put(GEPConstants.MAX_SCORE, rubricGrading.get(AJEntityRubricGrading.MAX_SCORE));
    	
    	resEvent.put(GEPConstants.RESULT, result);

    	return resEvent;
    	
    }
    
    private void sendCollScoreUpdateEventtoGEP(String cType) {

    	JsonObject gepEvent = createCollScoreUpdateEvent(cType);
    	
    	try {
    		LOGGER.debug("The Collection GEP Event is : {} ", gepEvent);
    		MessageDispatcher.getInstance().sendGEPEvent2Kafka(TOPIC_GEP_USAGE_EVENTS, gepEvent);
    		LOGGER.info("Successfully dispatched Collection Perf GEP Event..");
    	} catch (Exception e) {
    		LOGGER.error("Error while dispatching Collection Perf GEP Event ", e);
    	}
    }
    
    private JsonObject createCollScoreUpdateEvent(String collectionType) {    	
    	JsonObject cpEvent = new JsonObject();
    	JsonObject context = new JsonObject();
    	JsonObject result = new JsonObject();

    	cpEvent.put(GEPConstants.USER_ID, rubricGrading.get(AJEntityRubricGrading.STUDENT_ID));
    	cpEvent.put(GEPConstants.ACTIVITY_TIME, System.currentTimeMillis());
    	cpEvent.put(GEPConstants.EVENT_ID, UUID.randomUUID().toString());
    	cpEvent.put(GEPConstants.EVENT_NAME, GEPConstants.COLL_SCORE_UPDATE_EVENT);
    	cpEvent.put(GEPConstants.COLLECTION_ID, rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID));
    	cpEvent.put(GEPConstants.COLLECTION_TYPE, collectionType);

    	context.put(GEPConstants.CLASS_ID, rubricGrading.get(AJEntityRubricGrading.CLASS_ID));
    	context.put(GEPConstants.COURSE_ID, rubricGrading.get(AJEntityRubricGrading.COURSE_ID));
    	context.put(GEPConstants.UNIT_ID, rubricGrading.get(AJEntityRubricGrading.UNIT_ID));
    	context.put(GEPConstants.LESSON_ID, rubricGrading.get(AJEntityRubricGrading.LESSON_ID));
    	context.put(GEPConstants.SESSION_ID, rubricGrading.get(AJEntityRubricGrading.SESSION_ID));

    	cpEvent.put(GEPConstants.CONTEXT, context);
    	
        if (score != null && max_score != null && max_score > 0.0) {
        	//score = ((score * 100) / max_score);
        	result.put(GEPConstants.SCORE, score);
        	result.put(GEPConstants.MAX_SCORE, max_score);
          } else {
          	result.putNull(GEPConstants.SCORE);
          	result.put(GEPConstants.MAX_SCORE, max_score);
          }        
        
        cpEvent.put(GEPConstants.RESULT, result);
        
    	return cpEvent;
    	
    }
}
