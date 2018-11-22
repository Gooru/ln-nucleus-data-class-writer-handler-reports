package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.GEPConstants;
import org.gooru.nucleus.handlers.insights.events.processors.MessageDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.LTIEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.TeacherScoreOverideEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.EntityBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


/**
 * Created by renuka@gooru 
 * 
 */

public class DCAScoreUpdateHandler implements DBHandler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DCAScoreUpdateHandler.class);
	//TODO: This Kafka Topic name needs to be picked up from config
	public static final String TOPIC_GEP_USAGE_EVENTS = "org.gooru.da.sink.logW.usage.events";
	public static final String TOPIC_NOTIFICATIONS = "notifications";
	private static final String USER_ID_FROM_SESSION = "userIdFromSession";
	private static final String STUDENT_ID = "student_id";
	private static final String RESOURCES = "resources";
	private static final String CORRECT = "correct";
	private final ProcessorContext context;
	private AJEntityDailyClassActivity dcaReports;
	private String studentId;
	private Double score;
	private Double max_score;
	private Double rawScore;
	private Long pathId;
	private String pathType;
	private String contextCollectionId;
	private String contextCollectionType;
    private Boolean isGraded;
    private Long timeSpent = 0L;
    private String updated_at;
    private String tenantId;
    private String partnerId;

    public DCAScoreUpdateHandler(ProcessorContext context) {
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

         if (StringUtil.isNullOrEmpty(context.request().getString(STUDENT_ID)) ||
             StringUtil.isNullOrEmpty(context.request().getString(AJEntityReporting.CLASS_GOORU_OID)) || StringUtil.isNullOrEmpty(context.request().getString(AJEntityReporting.SESSION_ID))) {
             return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"), ExecutionStatus.FAILED);
         }
        LOGGER.debug("checkSanity() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @SuppressWarnings("rawtypes")
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
    
        dcaReports = new AJEntityDailyClassActivity();   
        LazyList<AJEntityDailyClassActivity> allGraded = null;
        JsonObject req = context.request(); 
        req.remove(USER_ID_FROM_SESSION);
        studentId = req.getString(STUDENT_ID);
        req.remove(STUDENT_ID);        
        JsonArray resources = req.getJsonArray(RESOURCES);
        
        List<JsonObject> jObj = IntStream.range(0, resources.size())
                .mapToObj(index -> (JsonObject) resources.getValue(index))
                .collect(Collectors.toList());
        req.remove(RESOURCES);        
        
        new DefAJEntityDailyClassActivityBuilder().build(dcaReports, req, AJEntityDailyClassActivity.getConverterRegistry());
          
        dcaReports.set(AJEntityDailyClassActivity.GOORUUID, studentId);
          //Since this NOT a Free-Response Question, the Max_Score can be safely assumed to be 1.0. So no need to update the same.
          jObj.forEach(attr -> {          
          String ans_status = attr.getString(AJEntityDailyClassActivity.RESOURCE_ATTEMPT_STATUS);          
          Base.exec(AJEntityDailyClassActivity.UPDATE_QUESTION_SCORE_U, ans_status.equalsIgnoreCase(CORRECT) ? 1.0 : 0.0,
                  true, attr.getString(AJEntityDailyClassActivity.RESOURCE_ATTEMPT_STATUS), studentId, dcaReports.get(AJEntityDailyClassActivity.CLASS_GOORU_OID),                   
                  dcaReports.get(AJEntityDailyClassActivity.SESSION_ID), 
                  dcaReports.get(AJEntityDailyClassActivity.COLLECTION_OID),
                  attr.getString(AJEntityDailyClassActivity.RESOURCE_ID));                 
          });
         
        	  LOGGER.debug("Computing total score...");
              LazyList<AJEntityDailyClassActivity> scoreTS = AJEntityDailyClassActivity.findBySQL(AJEntityDailyClassActivity.COMPUTE_ASSESSMENT_SCORE_POST_GRADING_U, 
            		  studentId, dcaReports.get(AJEntityDailyClassActivity.CLASS_GOORU_OID), dcaReports.get(AJEntityDailyClassActivity.COLLECTION_OID), 
            		  dcaReports.get(AJEntityDailyClassActivity.SESSION_ID));
              LOGGER.debug("scoreTS {} ", scoreTS);

              if (scoreTS != null && !scoreTS.isEmpty()) {	
                scoreTS.forEach(m -> {
                  rawScore = (m.get(AJEntityDailyClassActivity.SCORE) != null ? Double.valueOf(m.get(AJEntityDailyClassActivity.SCORE).toString()) : null);
                  LOGGER.debug("rawScore {} ", rawScore);
                  max_score = (m.get(AJEntityDailyClassActivity.MAX_SCORE) != null ? Double.valueOf(m.get(AJEntityDailyClassActivity.MAX_SCORE).toString()) : null);
                  LOGGER.debug("max_score {} ", max_score);        
                });
                            
                if (rawScore != null && max_score != null && max_score > 0.0) {
                  score = ((rawScore * 100) / max_score);
                  LOGGER.debug("Re-Computed total Assessment score {} ", score);
                }
              }
              Base.exec(AJEntityDailyClassActivity.UPDATE_ASSESSMENT_SCORE_U, score, max_score, studentId, dcaReports.get(AJEntityDailyClassActivity.CLASS_GOORU_OID),
            		  dcaReports.get(AJEntityDailyClassActivity.SESSION_ID), dcaReports.get(AJEntityDailyClassActivity.COLLECTION_OID));
              LOGGER.debug("DCA:Total score updated successfully...");

          	  AJEntityDailyClassActivity pathIdTypeModel =  AJEntityDailyClassActivity.findFirst("actor_id = ? AND class_id = ? AND course_id = ? AND unit_id = ? "
          			  + "AND lesson_id = ? AND collection_id = ? AND event_name = 'collection.play' AND event_type = 'stop'", 
          			dcaReports.get(AJEntityDailyClassActivity.GOORUUID), dcaReports.get(AJEntityDailyClassActivity.CLASS_GOORU_OID),
          			dcaReports.get(AJEntityDailyClassActivity.COURSE_GOORU_OID), dcaReports.get(AJEntityDailyClassActivity.UNIT_GOORU_OID), 
          			dcaReports.get(AJEntityDailyClassActivity.LESSON_GOORU_OID), dcaReports.get(AJEntityDailyClassActivity.COLLECTION_OID));
          			
          	if (pathIdTypeModel != null) {
        		pathType = pathIdTypeModel.get(AJEntityDailyClassActivity.PATH_TYPE) != null ? pathIdTypeModel.get(AJEntityDailyClassActivity.PATH_TYPE).toString() : null;
        		pathId = pathIdTypeModel.get(AJEntityDailyClassActivity.PATH_ID) != null ? Long.valueOf(pathIdTypeModel.get(AJEntityDailyClassActivity.PATH_ID).toString()) : 0L;
        		contextCollectionId = pathIdTypeModel.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_ID) != null ? pathIdTypeModel.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_ID).toString() : null;
        		contextCollectionType = pathIdTypeModel.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_TYPE) != null ? pathIdTypeModel.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_TYPE).toString() : null;
              	//Set these values into the baseReports Model for data injection into GEP/Notification Events
              	dcaReports.set(AJEntityDailyClassActivity.PATH_TYPE, pathType);
              	dcaReports.set(AJEntityDailyClassActivity.PATH_ID, pathId);
              	dcaReports.set(AJEntityDailyClassActivity.CONTEXT_COLLECTION_ID, contextCollectionId);
              	dcaReports.set(AJEntityDailyClassActivity.CONTEXT_COLLECTION_TYPE, contextCollectionType);
              	updated_at = pathIdTypeModel.get(AJEntityDailyClassActivity.UPDATE_TIMESTAMP).toString();
              	partnerId = pathIdTypeModel.get(AJEntityDailyClassActivity.PARTNER_ID) != null ? pathIdTypeModel.get(AJEntityDailyClassActivity.PARTNER_ID).toString() : null;
              	tenantId = pathIdTypeModel.get(AJEntityDailyClassActivity.TENANT_ID) != null ? pathIdTypeModel.get(AJEntityDailyClassActivity.TENANT_ID).toString() : null;
              	dcaReports.set(AJEntityDailyClassActivity.PARTNER_ID, partnerId);
              	dcaReports.set(AJEntityDailyClassActivity.TENANT_ID, tenantId);
        	} else { //This Case should NEVER Arise
              	dcaReports.set(AJEntityDailyClassActivity.PATH_TYPE, null);
              	dcaReports.set(AJEntityDailyClassActivity.PATH_ID, 0L);
              	dcaReports.set(AJEntityDailyClassActivity.CONTEXT_COLLECTION_ID, null);
              	dcaReports.set(AJEntityDailyClassActivity.CONTEXT_COLLECTION_TYPE, null);        		
        	}
          	
              //Send Score Update Events to GEP
              jObj.forEach(attr -> {          
                  String ans_status = attr.getString(AJEntityDailyClassActivity.RESOURCE_ATTEMPT_STATUS);
                  sendResourceScoreUpdateEventtoGEP(ans_status, attr.getString(AJEntityDailyClassActivity.RESOURCE_ID));                 
                  });
              
              TeacherScoreOverideEventDispatcher eventDispatcher = new TeacherScoreOverideEventDispatcher(dcaReports); 
              eventDispatcher.sendDCATeacherScoreUpdateEventtoNotifications();
              //Send Assessment Score update Event if ALL Questions have been graded
          	  allGraded =  AJEntityDailyClassActivity.findBySQL(AJEntityDailyClassActivity.IS_COLLECTION_GRADED, studentId, dcaReports.get(AJEntityDailyClassActivity.SESSION_ID), 
                      dcaReports.get(AJEntityDailyClassActivity.COLLECTION_OID), EventConstants.COLLECTION_RESOURCE_PLAY, EventConstants.STOP, false);
              if (allGraded == null || allGraded.isEmpty()) {
            	  isGraded = true;
            	  sendCollScoreUpdateEventtoGEP(); 
              } else {
                  isGraded = false;
              }  

        	  //We need to fetch the C/A Timespent, since the LTI-SBL event structure needs to be the same irrespective of the eventType
        	  //& the assumption is that duplicate events will be overlayed onto each other and so should include ALL the KPIs
              Object tsObject =  Base.firstCell(AJEntityDailyClassActivity.COMPUTE_TIMESPENT, dcaReports.get(AJEntityDailyClassActivity.COLLECTION_OID), 
            		  dcaReports.get(AJEntityDailyClassActivity.SESSION_ID));
              timeSpent = tsObject != null ? Long.valueOf(tsObject.toString()) : 0L;
        	  LTIEventDispatcher ltiEventDispatcher = new LTIEventDispatcher(dcaReports, timeSpent, updated_at, rawScore, max_score, score, isGraded);
        	  ltiEventDispatcher.sendDCATeacherOverrideEventtoLTI();
              
        LOGGER.debug("DONE");
        return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(), ExecutionStatus.SUCCESSFUL);
      }   
    
    private static class DefAJEntityDailyClassActivityBuilder implements EntityBuilder<AJEntityDailyClassActivity> {
    }
    

    @Override
    public boolean handlerReadOnly() {
        return false;
    }
    
    //********************************************************
    //TODO: Move GEP Event Processing to the EventDispatcher 
    //********************************************************
    
    private void sendResourceScoreUpdateEventtoGEP(String ansStatus, String resId) {    	
    	JsonObject result = new JsonObject();
    	JsonObject gepEvent = createResourceScoreUpdateEvent(resId);
    	
    	result.put(GEPConstants.SCORE, ansStatus.equalsIgnoreCase(CORRECT) ? 1.0 : 0.0);
    	result.put(GEPConstants.MAX_SCORE, 1.0);
    	
    	gepEvent.put(GEPConstants.RESULT, result);
    	
    	try {
    		LOGGER.debug("DCA:The Resource Update GEP Event due to Teacher Override is : {} ", gepEvent);
    		MessageDispatcher.getInstance().sendGEPEvent2Kafka(TOPIC_GEP_USAGE_EVENTS, gepEvent);
    		LOGGER.info("DCA:Successfully dispatched Resource Update GEP Event due to Teacher Override..");
    	} catch (Exception e) {
    		LOGGER.error("DCA:Error while dispatching Resource Update GEP Event due to Teacher Override ", e);
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

    	context.put(GEPConstants.CLASS_ID, dcaReports.get(AJEntityDailyClassActivity.CLASS_GOORU_OID));
    	context.put(GEPConstants.COURSE_ID, dcaReports.get(AJEntityDailyClassActivity.COURSE_GOORU_OID));
    	context.put(GEPConstants.UNIT_ID, dcaReports.get(AJEntityDailyClassActivity.UNIT_GOORU_OID));
    	context.put(GEPConstants.LESSON_ID, dcaReports.get(AJEntityDailyClassActivity.LESSON_GOORU_OID));
    	context.put(GEPConstants.COLLECTION_ID, dcaReports.get(AJEntityDailyClassActivity.COLLECTION_OID));
    	context.put(GEPConstants.COLLECTION_TYPE, dcaReports.get(AJEntityDailyClassActivity.COLLECTION_TYPE));

    	context.put(GEPConstants.PATH_TYPE, dcaReports.get(AJEntityDailyClassActivity.PATH_TYPE));
    	context.put(GEPConstants.PATH_ID, dcaReports.get(AJEntityDailyClassActivity.PATH_ID));
    	
    	context.put(GEPConstants.CONTEXT_COLLECTION_ID, dcaReports.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_ID));
    	context.put(GEPConstants.CONTEXT_COLLECTION_TYPE, dcaReports.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_TYPE));

    	context.put(GEPConstants.SESSION_ID, dcaReports.get(AJEntityDailyClassActivity.SESSION_ID));

    	resEvent.put(GEPConstants.CONTEXT, context);

    	return resEvent;
    	
    }

    private void sendCollScoreUpdateEventtoGEP() {
    	JsonObject gepEvent = createCollScoreUpdateEvent();
    	
    	try {
    		LOGGER.debug("DCA:The Collection GEP Event is : {} ", gepEvent);
    		MessageDispatcher.getInstance().sendGEPEvent2Kafka(TOPIC_GEP_USAGE_EVENTS, gepEvent);
    		LOGGER.info("DCA:Successfully dispatched Collection Perf GEP Event..");
    	} catch (Exception e) {
    		LOGGER.error("DCA:Error while dispatching Collection Perf GEP Event ", e);
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
    	cpEvent.put(GEPConstants.COLLECTION_ID, dcaReports.get(AJEntityDailyClassActivity.COLLECTION_OID));
    	cpEvent.put(GEPConstants.COLLECTION_TYPE, dcaReports.get(AJEntityDailyClassActivity.COLLECTION_TYPE));

    	context.put(GEPConstants.CLASS_ID, dcaReports.get(AJEntityDailyClassActivity.CLASS_GOORU_OID));
    	context.put(GEPConstants.COURSE_ID, dcaReports.get(AJEntityDailyClassActivity.COURSE_GOORU_OID));
    	context.put(GEPConstants.UNIT_ID, dcaReports.get(AJEntityDailyClassActivity.UNIT_GOORU_OID));
    	context.put(GEPConstants.LESSON_ID, dcaReports.get(AJEntityDailyClassActivity.LESSON_GOORU_OID));
    	context.put(GEPConstants.SESSION_ID, dcaReports.get(AJEntityDailyClassActivity.SESSION_ID));
    	
    	context.put(GEPConstants.PATH_TYPE, dcaReports.get(AJEntityDailyClassActivity.PATH_TYPE));
    	context.put(GEPConstants.PATH_ID, dcaReports.get(AJEntityDailyClassActivity.PATH_ID));
    	
    	context.put(GEPConstants.CONTEXT_COLLECTION_ID, dcaReports.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_ID));
    	context.put(GEPConstants.CONTEXT_COLLECTION_TYPE, dcaReports.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_TYPE));

    	cpEvent.put(GEPConstants.CONTEXT, context);
    	
        if (score != null && max_score != null && max_score > 0.0) {
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
