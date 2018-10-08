package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;

import org.gooru.nucleus.handlers.insights.events.constants.LTIConstants;
import org.gooru.nucleus.handlers.insights.events.processors.MessageDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.events.EventParser;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityRubricGrading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

/**
 * Created by mukul@gooru
 */

public class LTIEventDispatcher {

	private static final Logger LOGGER = LoggerFactory.getLogger(LTIEventDispatcher.class);
	public static final String TOPIC_NOTIFICATIONS = "notifications";
	private AJEntityReporting baseReports;
	private AJEntityRubricGrading rubricGrading;
	private EventParser event;
	private Double rawScore;
	private Double maxScore;
	private Double score;
	private Boolean isGraded;
	private Long timeSpent;
	private String updated_at;

	public LTIEventDispatcher (AJEntityReporting baseReports, EventParser event, 
			Double rawScore, Double maxScore, Double score, Boolean isGraded) {
		this.baseReports = baseReports;
		this.event = event;
		this.rawScore = rawScore;
		this.maxScore = maxScore;
		this.score = score;
		this.isGraded = isGraded;
	}
	
	public LTIEventDispatcher (AJEntityRubricGrading rubricGrading,  
			Long timeSpent, String updated_at, Double rawScore, Double maxScore, Double score, Boolean isGraded) {
		this.rubricGrading = rubricGrading;
		this.timeSpent = timeSpent;
		this.rawScore = rawScore;
		this.maxScore = maxScore;
		this.score = score;
		this.isGraded = isGraded;
		this.updated_at = updated_at;
	}
	
	public LTIEventDispatcher (AJEntityReporting baseReports,  
			Long timeSpent, String updated_at, Double rawScore, Double maxScore, Double score, Boolean isGraded) {
		this.baseReports = baseReports;
		this.timeSpent = timeSpent;
		this.rawScore = rawScore;
		this.maxScore = maxScore;
		this.score = score;
		this.isGraded = isGraded;
		this.updated_at = updated_at;
	}
	
	  public void sendCollPerfEventtoLTI() {
		    JsonObject ltiEvent = createCollPerfEventtoLTI();
		    
		    try {
		      LOGGER.debug("LTI Event : {} ", ltiEvent);
		      MessageDispatcher.getInstance().sendMessage2Kafka(ltiEvent);
		      LOGGER.info("Successfully dispatched LTI message..");
		    } catch (Exception e) {
		      LOGGER.error("Error while dispatching LTI message ", e);
		    }
		  }
	  
	  private JsonObject createCollPerfEventtoLTI() {    
		
		  JsonObject collPerfEvent = new JsonObject();
		  collPerfEvent.put("eventType", LTIConstants.STUDENT_ACTIVITY);
		  collPerfEvent.put("userUid", baseReports.get(AJEntityReporting.GOORUUID));
		  collPerfEvent.put("contentGooruId", baseReports.get(AJEntityReporting.COLLECTION_OID));
		  collPerfEvent.put("classGooruId", baseReports.get(AJEntityReporting.CLASS_GOORU_OID));
		  collPerfEvent.put("courseGooruId",baseReports.get(AJEntityReporting.COURSE_GOORU_OID));
		  collPerfEvent.put("unitGooruId", baseReports.get(AJEntityReporting.UNIT_GOORU_OID));
		  collPerfEvent.put("lessonGooruId", baseReports.get(AJEntityReporting.LESSON_GOORU_OID));
		  collPerfEvent.put("type", baseReports.get(AJEntityReporting.COLLECTION_TYPE));
		  collPerfEvent.put("timeSpentInMs", baseReports.get(AJEntityReporting.TIMESPENT));
		  collPerfEvent.put("scoreInPercentage", score);
		  collPerfEvent.put("rawScore", rawScore);
		  collPerfEvent.put("maxScore", maxScore);
		  collPerfEvent.put("reaction", 0);
		  collPerfEvent.put("completedTime", event.getEndTime());
//		  collPerfEvent.put("isStudent",event.isStudent());
		  collPerfEvent.put("accessToken", event.getAccessToken());
		  collPerfEvent.put("sourceId", event.getSourceId());
//		  collPerfEvent.put("questionsCount", baseReports.get(AJEntityReporting.QUESTION_COUNT));
		  collPerfEvent.put("partnerId", baseReports.get(AJEntityReporting.PARTNER_ID));
		  collPerfEvent.put("tenantId", baseReports.get(AJEntityReporting.TENANT_ID));
		  if (isGraded){
			  collPerfEvent.put("gradingStatus", "complete");
		  } else {
			  collPerfEvent.put("gradingStatus", "pending");
		  }
		  return collPerfEvent;		  
		}  

	  public void sendTeacherGradingEventtoLTI() {
		    JsonObject ltiEvent = createTeacherGradingEventtoLTI();

		    try {
		      LOGGER.debug("Teacher Grading LTI Event : {} ", ltiEvent);
		      MessageDispatcher.getInstance().sendMessage2Kafka(ltiEvent);
		      LOGGER.info("Successfully dispatched Teacher Grading LTI message..");
		    } catch (Exception e) {
		      LOGGER.error("Error while dispatching Teacher Grading LTI message ", e);
		    }
		  }
	  
	  private JsonObject createTeacherGradingEventtoLTI() {    
			
		  JsonObject teacherGradingEvent = new JsonObject();
		  teacherGradingEvent.put("eventType", LTIConstants.TEACHER_GRADING_COMPLETE);
		  teacherGradingEvent.put("userUid", rubricGrading.get(AJEntityRubricGrading.STUDENT_ID));
		  teacherGradingEvent.put("contentGooruId", rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID));
		  teacherGradingEvent.put("classGooruId", rubricGrading.get(AJEntityRubricGrading.CLASS_ID));
		  teacherGradingEvent.put("courseGooruId", rubricGrading.get(AJEntityRubricGrading.COURSE_ID));
		  teacherGradingEvent.put("unitGooruId", rubricGrading.get(AJEntityRubricGrading.UNIT_ID));
		  teacherGradingEvent.put("lessonGooruId", rubricGrading.get(AJEntityRubricGrading.LESSON_ID));
		  teacherGradingEvent.put("type", rubricGrading.get(AJEntityRubricGrading.COLLECTION_TYPE));
		  teacherGradingEvent.put("timeSpentInMs", timeSpent);
		  teacherGradingEvent.put("scoreInPercentage", score);
		  teacherGradingEvent.put("rawScore", rawScore);
		  teacherGradingEvent.put("maxScore", maxScore);
		  teacherGradingEvent.put("reaction", 0);
		  
		  DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
				  .withResolverStyle(ResolverStyle.LENIENT);
		  LocalDateTime dt = LocalDateTime.parse(updated_at, dtf);
		  Instant instant = dt.toInstant(ZoneOffset.UTC);
		  		  
		  teacherGradingEvent.put("completedTime", instant.toEpochMilli());
		  teacherGradingEvent.putNull("accessToken");
		  teacherGradingEvent.putNull("sourceId");
		  teacherGradingEvent.put("partnerId", baseReports.get(AJEntityReporting.PARTNER_ID));
		  teacherGradingEvent.put("tenantId", baseReports.get(AJEntityReporting.TENANT_ID));
		  if (isGraded){
			  teacherGradingEvent.put("gradingStatus", "complete");
		  } else {
			  teacherGradingEvent.put("gradingStatus", "pending");
		  }
		  return teacherGradingEvent;		  
		}  
	  
	  public void sendTeacherOverrideEventtoLTI() {
		    JsonObject ltiEvent = createTeacherOverrideEventtoLTI();

		    try {
		      LOGGER.debug("Teacher Grading LTI Event : {} ", ltiEvent);
		      MessageDispatcher.getInstance().sendMessage2Kafka(ltiEvent);
		      LOGGER.info("Successfully dispatched Teacher Grading LTI message..");
		    } catch (Exception e) {
		      LOGGER.error("Error while dispatching Teacher Grading LTI message ", e);
		    }
		  }
	  
	  private JsonObject createTeacherOverrideEventtoLTI() {    
			
		  JsonObject teacherOverrideEvent = new JsonObject();
		  teacherOverrideEvent.put("eventType", LTIConstants.TEACHER_SCORE_OVERRIDE);
		  teacherOverrideEvent.put("userUid", baseReports.get(AJEntityReporting.GOORUUID));
		  teacherOverrideEvent.put("contentGooruId", baseReports.get(AJEntityReporting.COLLECTION_OID));
		  teacherOverrideEvent.put("classGooruId", baseReports.get(AJEntityReporting.CLASS_GOORU_OID));
		  teacherOverrideEvent.put("courseGooruId",baseReports.get(AJEntityReporting.COURSE_GOORU_OID));
		  teacherOverrideEvent.put("unitGooruId", baseReports.get(AJEntityReporting.UNIT_GOORU_OID));
		  teacherOverrideEvent.put("lessonGooruId", baseReports.get(AJEntityReporting.LESSON_GOORU_OID));
		  teacherOverrideEvent.put("type", baseReports.get(AJEntityReporting.COLLECTION_TYPE));
		  teacherOverrideEvent.put("timeSpentInMs", baseReports.get(AJEntityReporting.TIMESPENT));
		  teacherOverrideEvent.put("scoreInPercentage", score);
		  teacherOverrideEvent.put("rawScore", rawScore);
		  teacherOverrideEvent.put("maxScore", maxScore);		  
		  teacherOverrideEvent.put("reaction", 0);
		  
		  DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
				  .withResolverStyle(ResolverStyle.LENIENT);
		  LocalDateTime dt = LocalDateTime.parse(updated_at, dtf);
		  Instant instant = dt.toInstant(ZoneOffset.UTC);
		  
		  teacherOverrideEvent.put("completedTime", instant.toEpochMilli());
		  teacherOverrideEvent.putNull("accessToken");
		  teacherOverrideEvent.putNull("sourceId");
		  teacherOverrideEvent.put("partnerId", baseReports.get(AJEntityReporting.PARTNER_ID));
		  teacherOverrideEvent.put("tenantId", baseReports.get(AJEntityReporting.TENANT_ID));
		  if (isGraded){
			  teacherOverrideEvent.put("gradingStatus", "complete");
		  } else {
			  teacherOverrideEvent.put("gradingStatus", "pending");
		  }
		  return teacherOverrideEvent;		  
		}  

}
