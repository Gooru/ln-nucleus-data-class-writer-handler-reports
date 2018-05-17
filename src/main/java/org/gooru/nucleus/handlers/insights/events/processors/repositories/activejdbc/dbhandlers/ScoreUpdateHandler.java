package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.GEPConstants;
import org.gooru.nucleus.handlers.insights.events.processors.MessageDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityRubricGrading;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.EntityBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


/**
 * Created by mukul@gooru 
 * 
 */

public class ScoreUpdateHandler implements DBHandler {
	
  private static final Logger LOGGER = LoggerFactory.getLogger(ScoreUpdateHandler.class);
  public static final String TOPIC_GEP_USAGE_EVENTS = "org.gooru.da.sink.logW.usage.events";
  private static final String USER_ID_FROM_SESSION = "userIdFromSession";
  private static final String STUDENT_ID = "student_id";
  private static final String RESOURCES = "resources";
  private static final String CORRECT = "correct";
  private static final String INCORRECT = "incorrect";  
  private final ProcessorContext context;
  private AJEntityReporting BaseReports;
  private String studentId;
  private Double score;
  private Double max_score;

    public ScoreUpdateHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
         if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("Invalid Data");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid Data"),
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
    
        BaseReports = new AJEntityReporting();   
        LazyList<AJEntityReporting> allGraded = null;
        JsonObject req = context.request(); 
        String teacherId = req.getString(USER_ID_FROM_SESSION);
        req.remove(USER_ID_FROM_SESSION);
        studentId = req.getString(STUDENT_ID);
        req.remove(STUDENT_ID);        
        JsonArray resources = req.getJsonArray(RESOURCES);
        
        List<JsonObject> jObj = IntStream.range(0, resources.size())
                .mapToObj(index -> (JsonObject) resources.getValue(index))
                .collect(Collectors.toList());
        req.remove(RESOURCES);        
        
        new DefAJEntityReportingBuilder().build(BaseReports, req, AJEntityReporting.getConverterRegistry());
        
        //TODO: Create a Validator functions to add validations for attributes
        if (BaseReports.get(AJEntityReporting.CLASS_GOORU_OID) == null || BaseReports.get(AJEntityReporting.SESSION_ID) == null 
        		|| BaseReports.get(AJEntityReporting.COLLECTION_OID) == null) {
        	
            return new ExecutionResult<>(
                    MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
                    ExecutionStatus.FAILED);        	
        }
          
          //Since this NOT a Free-Response Question, the Max_Score can be safely assumed to be 1.0. So no need to update the same.
          jObj.forEach(attr -> {          
          String ans_status = attr.getString(AJEntityReporting.RESOURCE_ATTEMPT_STATUS);          
          Base.exec(AJEntityReporting.UPDATE_QUESTION_SCORE_U, ans_status.equalsIgnoreCase(CORRECT) ? 1.0 : 0.0,
                  true, attr.getString(AJEntityReporting.RESOURCE_ATTEMPT_STATUS), studentId, BaseReports.get(AJEntityReporting.CLASS_GOORU_OID),                   
                  BaseReports.get(AJEntityReporting.SESSION_ID), 
                  BaseReports.get(AJEntityReporting.COLLECTION_OID),
                  attr.getString(AJEntityReporting.RESOURCE_ID));                 
          });
         
        	  LOGGER.debug("Computing total score...");
              LazyList<AJEntityReporting> scoreTS = AJEntityReporting.findBySQL(AJEntityReporting.COMPUTE_ASSESSMENT_SCORE_POST_GRADING_U, 
            		  studentId, BaseReports.get(AJEntityReporting.CLASS_GOORU_OID), BaseReports.get(AJEntityReporting.COLLECTION_OID), 
            		  BaseReports.get(AJEntityReporting.SESSION_ID));
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
                  LOGGER.debug("Re-Computed total Assessment score {} ", score);
                }
              }
              Base.exec(AJEntityReporting.UPDATE_ASSESSMENT_SCORE_U, score, max_score, studentId, BaseReports.get(AJEntityReporting.CLASS_GOORU_OID),
            		  BaseReports.get(AJEntityReporting.SESSION_ID), BaseReports.get(AJEntityReporting.COLLECTION_OID));
              LOGGER.debug("Total score updated successfully...");
      
              //Send Score Update Events to GEP
              jObj.forEach(attr -> {          
                  String ans_status = attr.getString(AJEntityReporting.RESOURCE_ATTEMPT_STATUS);
                  sendResourceScoreUpdateEventtoGEP(ans_status, attr.getString(AJEntityReporting.RESOURCE_ID));                 
                  });
              
              //Send Assessment Score update Event if ALL Questions have been graded
          	  allGraded =  AJEntityReporting.findBySQL(AJEntityReporting.IS_COLLECTION_GRADED, studentId, BaseReports.get(AJEntityReporting.SESSION_ID), 
                      BaseReports.get(AJEntityReporting.COLLECTION_OID), EventConstants.COLLECTION_RESOURCE_PLAY, EventConstants.STOP, false);
              if (allGraded == null || allGraded.isEmpty()) {
            	  sendCollScoreUpdateEventtoGEP();  
              }
              
        LOGGER.debug("DONE");
        return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(), ExecutionStatus.SUCCESSFUL);
      }   
    
    private static class DefAJEntityReportingBuilder implements EntityBuilder<AJEntityReporting> {
    }
    

    @Override
    public boolean handlerReadOnly() {
        return false;
    }
      
    
    private void sendResourceScoreUpdateEventtoGEP(String ansStatus, String resId) {    	
    	JsonObject result = new JsonObject();
    	JsonObject gepEvent = createResourceScoreUpdateEvent(resId);
    	
    	result.put(GEPConstants.SCORE, ansStatus.equalsIgnoreCase(CORRECT) ? 1.0 : 0.0);
    	result.put(GEPConstants.MAX_SCORE, 1.0);
    	
    	gepEvent.put(GEPConstants.RESULT, result);
    	
    	try {
    		LOGGER.debug("The Resource Update GEP Event due to Teacher Override is : {} ", gepEvent);
    		MessageDispatcher.getInstance().sendGEPEvent2Kafka(TOPIC_GEP_USAGE_EVENTS, gepEvent);
    		LOGGER.info("Successfully dispatched Resource Update GEP Event due to Teacher Override..");
    	} catch (Exception e) {
    		LOGGER.error("Error while dispatching Resource Update GEP Event due to Teacher Override ", e);
    	}
    }
    
    private JsonObject createResourceScoreUpdateEvent(String rId) {    	
    	JsonObject resEvent = new JsonObject();
    	JsonObject context = new JsonObject();


    	resEvent.put(GEPConstants.USER_ID, studentId);
    	resEvent.put(GEPConstants.ACTIVITY_TIME, System.currentTimeMillis());
    	resEvent.put(GEPConstants.EVENT_ID, UUID.randomUUID().toString());
    	resEvent.put(GEPConstants.EVENT_NAME, GEPConstants.RES_SCORE_UPDATE_EVENT);
    	resEvent.put(GEPConstants.RESOURCE_ID, rId);
    	resEvent.put(GEPConstants.RESOURCE_TYPE, GEPConstants.QUESTION);

    	context.put(GEPConstants.CLASS_ID, BaseReports.get(AJEntityReporting.CLASS_GOORU_OID));
    	context.put(GEPConstants.COURSE_ID, BaseReports.get(AJEntityReporting.COURSE_GOORU_OID));
    	context.put(GEPConstants.UNIT_ID, BaseReports.get(AJEntityReporting.UNIT_GOORU_OID));
    	context.put(GEPConstants.LESSON_ID, BaseReports.get(AJEntityReporting.LESSON_GOORU_OID));
    	context.put(GEPConstants.COLLECTION_ID, BaseReports.get(AJEntityReporting.COLLECTION_OID));
    	context.put(GEPConstants.COLLECTION_TYPE, BaseReports.get(AJEntityReporting.COLLECTION_TYPE));
    	context.put(GEPConstants.SESSION_ID, BaseReports.get(AJEntityReporting.SESSION_ID));

    	resEvent.put(GEPConstants.CONTEXT, context);

    	return resEvent;
    	
    }

    private void sendCollScoreUpdateEventtoGEP() {
    	JsonObject gepEvent = createCollScoreUpdateEvent();
    	
    	try {
    		LOGGER.debug("The Collection GEP Event is : {} ", gepEvent);
    		MessageDispatcher.getInstance().sendGEPEvent2Kafka(TOPIC_GEP_USAGE_EVENTS, gepEvent);
    		LOGGER.info("Successfully dispatched Collection Perf GEP Event..");
    	} catch (Exception e) {
    		LOGGER.error("Error while dispatching Collection Perf GEP Event ", e);
    	}
    }
    
    private JsonObject createCollScoreUpdateEvent() {    	
    	JsonObject cpEvent = new JsonObject();
    	JsonObject context = new JsonObject();
    	JsonObject result = new JsonObject();

    	cpEvent.put(GEPConstants.USER_ID, studentId);
    	//Since EventTime is not included in this event, we will use time during this event generation
    	cpEvent.put(GEPConstants.ACTIVITY_TIME, System.currentTimeMillis());
    	cpEvent.put(GEPConstants.EVENT_ID, UUID.randomUUID().toString());
    	cpEvent.put(GEPConstants.EVENT_NAME, GEPConstants.COLL_SCORE_UPDATE_EVENT);
    	cpEvent.put(GEPConstants.COLLECTION_ID, BaseReports.get(AJEntityReporting.COLLECTION_OID));
    	cpEvent.put(GEPConstants.COLLECTION_TYPE, BaseReports.get(AJEntityReporting.COLLECTION_TYPE));

    	context.put(GEPConstants.CLASS_ID, BaseReports.get(AJEntityReporting.CLASS_GOORU_OID));
    	context.put(GEPConstants.COURSE_ID, BaseReports.get(AJEntityReporting.COURSE_GOORU_OID));
    	context.put(GEPConstants.UNIT_ID, BaseReports.get(AJEntityReporting.UNIT_GOORU_OID));
    	context.put(GEPConstants.LESSON_ID, BaseReports.get(AJEntityReporting.LESSON_GOORU_OID));
    	context.put(GEPConstants.SESSION_ID, BaseReports.get(AJEntityReporting.SESSION_ID));

    	cpEvent.put(GEPConstants.CONTEXT, context);
    	
        if (score != null && max_score != null && max_score > 0.0) {
        	score = ((score * 100) / max_score);
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
