package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.sql.Timestamp;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.events.EventParser;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.GEPEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.RDAEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.utils.BaseUtil;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.util.StringUtil;
import io.vertx.core.json.JsonObject;

/**
 * created by mukul@gooru
 */

public class DailyClassActivityEventHandler implements DBHandler {


  private static final Logger LOGGER = LoggerFactory
      .getLogger(DailyClassActivityEventHandler.class);
  private final ProcessorContext context;
  private AJEntityDailyClassActivity dcaReport;
  private EventParser event;
  Double scoreObj;
  Double maxScoreObj;
  Long tsObj;

  Long views;
  Long reaction;
  Long timespent;
  Double maxScore;
  Double score;
  Boolean isGraded;
  
  public DailyClassActivityEventHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("invalid request received");
      return new ExecutionResult<>(
          MessageResponseFactory
              .createInvalidRequestResponse("Invalid data received to process events"),
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
    LazyList<AJEntityDailyClassActivity> scoreTS = null;
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

    dcaReport.set("resource_attempt_status", event.getAnswerStatus());
    dcaReport.set("views", event.getViews());
    dcaReport.set("time_spent", event.getTimespent());
    dcaReport.set("tenant_id", event.getTenantId());
    dcaReport.set("created_at", new Timestamp(event.getStartTime()));
    dcaReport.set("updated_at", new Timestamp(event.getEndTime()));

    dcaReport.set("max_score", event.getMaxScore());
    dcaReport.set("grading_type", event.getGradeType());
    dcaReport.set("app_id", event.getAppId());
    dcaReport.set("partner_id", event.getPartnerId());
    //pathId = 0L indicates the main Path. We store pathId only for the altPaths
    if (event.getPathId() != 0L) {
      dcaReport.set("path_id", event.getPathId());
      if (!StringUtil.isNullOrEmpty(event.getPathType())) {
        if (EventConstants.CA_PATH_TYPES.matcher(event.getPathType()).matches()) {
          dcaReport.set("path_type", event.getPathType());
        } else {
          LOGGER.warn("Invalid Path Type passed in event : {}", event.getPathType());
        }
      }
    }
    dcaReport.set("collection_sub_type", event.getCollectionSubType());
    dcaReport.set("event_id", event.getEventId());
    dcaReport.set("content_source", event.getContentSource());

    this.timespent = event.getTimespent();
    this.views = event.getViews();
    this.maxScore = event.getMaxScore();
    this.reaction = event.getReaction();
    
    if (event.getTimeZone() != null) {
      String timeZone = event.getTimeZone();
      LOGGER.debug("Timezone is " + timeZone);
      dcaReport.set("time_zone", timeZone);
      String localeDate = BaseUtil.UTCToLocale(event.getEndTime(), timeZone);

      if (localeDate != null) {
        dcaReport.setDateinTZ(localeDate);
      }
    }

    if (event.getContextCollectionId() != null) {
      dcaReport.set("context_collection_id", event.getContextCollectionId());
    }

    if (event.getContextCollectionType() != null) {
      dcaReport.set("context_collection_type", event.getContextCollectionType());
    }

    if ((event.getEventName().equals(EventConstants.COLLECTION_PLAY))) {
      duplicateRow = AJEntityDailyClassActivity
          .findBySQL(AJEntityDailyClassActivity.FIND_COLLECTION_EVENT, event.getGooruUUID(), event.getSessionId(),
              event.getContentGooruId(), event.getEventType(), event.getEventName());
      dcaReport.set("collection_id", event.getContentGooruId());
      dcaReport.set("question_count", event.getQuestionCount());
      if (event.getEventType().equalsIgnoreCase(EventConstants.START)) {
        dcaReport.set("score", event.getScore());
        score = event.getScore();
      }

      if (event.getEventType().equalsIgnoreCase(EventConstants.STOP)) {
        scoreTS = AJEntityDailyClassActivity
            .findBySQL(AJEntityDailyClassActivity.COMPUTE_ASSESSMENT_SCORE,
                event.getContentGooruId(),
                event.getSessionId());
        if (!scoreTS.isEmpty()) {
          scoreTS.forEach(m -> {
            //If ALL Questions in Assessments are Free Response Questions, awaiting grading, score will be NULL
            scoreObj = (m.get(AJEntityDailyClassActivity.SCORE) != null ?
                Double.valueOf(m.get(AJEntityDailyClassActivity.SCORE).toString()) : null);
            maxScoreObj = (m.get(AJEntityDailyClassActivity.MAX_SCORE) != null ?
                Double.valueOf(m.get(AJEntityDailyClassActivity.MAX_SCORE).toString()) : null);
            tsObj = Long.valueOf(m.get(AJEntityDailyClassActivity.TIMESPENT).toString());
          });

          //maxScore should be Null only in the case when all the questions in an Assessment are Free Response Question
          //In that case Score will not be calculated unless the questions are graded via the grading flow
          if (maxScoreObj != null && maxScoreObj != 0.0 && scoreObj != null) {
            dcaReport.set("score", ((scoreObj * 100) / maxScoreObj));
            dcaReport.set("max_score", maxScoreObj);
            maxScore = maxScoreObj;
            score = ((scoreObj * 100) / maxScoreObj);
          }

          if (event.getCollectionType().equalsIgnoreCase(EventConstants.ASSESSMENT)) {
            dcaReport.set("time_spent", (tsObj != null ? tsObj : 0));
            timespent = (tsObj != null ? tsObj : 0);
          }
        }
      }
    }

//    	if ((event.getEventName().equals(EventConstants.COLLECTION_RESOURCE_PLAY))) {
//    	  duplicateRow = AJEntityDailyClassActivity.findBySQL(AJEntityDailyClassActivity.FIND_RESOURCE_EVENT, 
//    			  event.getParentGooruId(), event.getSessionId(),event.getContentGooruId(),event.getEventType());
//    		dcaReport.set("collection_id", event.getParentGooruId());
//    		dcaReport.set("resource_id", event.getContentGooruId());    		
//    		dcaReport.set("answer_object", event.getAnswerObject().toString());
//    	}

    if ((event.getEventName().equals(EventConstants.COLLECTION_RESOURCE_PLAY))) {
      duplicateRow = AJEntityDailyClassActivity
          .findBySQL(AJEntityDailyClassActivity.FIND_RESOURCE_EVENT, event.getGooruUUID(), event.getParentGooruId(),
              event.getSessionId(), event.getContentGooruId(), event.getEventType());
      dcaReport.set("collection_id", event.getParentGooruId());
      dcaReport.set("resource_id", event.getContentGooruId());
      dcaReport.set("answer_object", event.getAnswerObject().toString());

      if (event.getResourceType().equals(EventConstants.QUESTION)) {
        if (event.getEventType().equalsIgnoreCase(EventConstants.START) && (event.getQuestionType()
            .equalsIgnoreCase(EventConstants.OE))) {
          dcaReport.set("grading_type", event.getGradeType());
        } else if (event.getEventType().equalsIgnoreCase(EventConstants.START)) {
          dcaReport.set("score", event.getScore());
          dcaReport.setBoolean("is_graded", true);
          this.score = event.getScore();
          this.isGraded = true;
        }
        if (event.getEventType().equalsIgnoreCase(EventConstants.STOP) && (
            event.getAnswerStatus().equalsIgnoreCase(EventConstants.INCORRECT)
                || event.getAnswerStatus().equalsIgnoreCase(EventConstants.CORRECT)
                || event.getAnswerStatus().equalsIgnoreCase(EventConstants.SKIPPED))) {
          //Grading Type is set by default to "system", so no need to update the grading_type here.
          dcaReport.set("score", event.getScore());
          dcaReport.setBoolean("is_graded", true);
          this.score = event.getScore();
          this.isGraded = true;
        } else if (event.getEventType().equalsIgnoreCase(EventConstants.STOP) &&
            (event.getAnswerStatus().equalsIgnoreCase(EventConstants.ATTEMPTED))) {
          dcaReport.set("grading_type", event.getGradeType());
          dcaReport.setBoolean("is_graded", false);
          this.isGraded = false;
        }
      }
    }

    if ((event.getEventName().equals(EventConstants.REACTION_CREATE))) {
      dcaReport.set("collection_id", event.getParentGooruId());
      dcaReport.set("resource_id", event.getContentGooruId());
    }
    if (dcaReport.hasErrors()) {
      LOGGER.warn("Errors in creating DCA Report");
    }
    LOGGER.info("Event, Before inserting into DCA Table: " + context.request().toString());

    if (dcaReport.isValid()) {
      if (duplicateRow == null || duplicateRow.isEmpty()) {
        if (dcaReport.insert()) {
          LOGGER.info("Record inserted successfully");
        } else {
          LOGGER.error("Error while inserting event: " + context.request().toString());
          return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
              ExecutionStatus.FAILED);
        }
      } else {
        LOGGER.debug("Found duplicate row. so updating duplicate row.....");
        if ((event.getEventName().equals(EventConstants.COLLECTION_RESOURCE_PLAY))) {
          duplicateRow.forEach(dup -> {
            int id = Integer.valueOf(dup.get("id").toString());
            long view = (Long.valueOf(dup.get("views").toString()) + event.getViews());
            long ts = (Long.valueOf(dup.get("time_spent").toString()) + event.getTimespent());
            long react = event.getReaction() != 0 ? event.getReaction() : 0;
            Double score = event.getScore();
            // update the Answer Object and Answer Status from the latest event
            // Rubrics - if the Answer Status is attempted then the default score that should be set
            // is null
            if (event.getResourceType().equals(EventConstants.QUESTION)
                && event.getEventType().equalsIgnoreCase(EventConstants.STOP)
                && event.getAnswerStatus().equalsIgnoreCase(EventConstants.ATTEMPTED)) {
              score = null;
            }
            Base.exec(AJEntityDailyClassActivity.UPDATE_RESOURCE_EVENT, view, ts, score,
                new Timestamp(event.getEndTime()), react, event.getAnswerStatus(),
                event.getAnswerObject().toString(), id);
            this.score = score;
            this.timespent = ts;
            this.views = view;
            this.reaction = react;
          });

        }
        if ((event.getEventName().equals(EventConstants.COLLECTION_PLAY))) {
          duplicateRow.forEach(dup -> {
            int id = Integer.valueOf(dup.get("id").toString());
            long view = (Long.valueOf(dup.get("views").toString()) + event.getViews());
            long ts = (Long.valueOf(dup.get("time_spent").toString()) + event.getTimespent());
            long react = event.getReaction() != 0 ? event.getReaction() : 0;
            double maxSco = event.getMaxScore();
            Double sco = event.getScore();
            if (event.getEventType().equalsIgnoreCase(EventConstants.STOP)) {
              if (maxScoreObj != null && maxScoreObj > 0.0 && scoreObj != null) {
                maxSco = maxScoreObj;
                sco = (scoreObj * 100) / maxSco;
              } else {
                sco = null;
              }
            }
            Base.exec(AJEntityDailyClassActivity.UPDATE_COLLECTION_EVENT, view, ts,
                sco, maxSco,
                new Timestamp(event.getEndTime()), react, id);
            this.score = sco;
            this.timespent = ts;
            this.views = view;
            this.reaction = react;
            this.maxScore = maxSco;
          });
        }
      }
    } else {
      LOGGER.warn("Event validation error");
      return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
          ExecutionStatus.FAILED);
    }

    if ((event.getEventName().equals(EventConstants.COLLECTION_PLAY)) && event.getEventType()
        .equalsIgnoreCase(EventConstants.START)) {
      sendCPStartEventToGEP(dcaReport);
      RDAEventDispatcher rdaEventDispatcher = new RDAEventDispatcher(dcaReport, this.views,
          this.reaction, this.timespent, this.maxScore, this.score, this.isGraded,
          this.event.getEndTime());
      rdaEventDispatcher.sendCollectionStartDCAEventToRDA();
    }

    if ((event.getEventName().equals(EventConstants.COLLECTION_PLAY)) && event.getEventType()
        .equalsIgnoreCase(EventConstants.STOP)) {
      // Send Collection Performance Event to GEP only if ALL the questions have been GRADED
      LazyList<AJEntityDailyClassActivity> allGraded =
          AJEntityDailyClassActivity.findBySQL(AJEntityDailyClassActivity.IS_COLLECTION_GRADED,
              event.getGooruUUID(), event.getSessionId(), event.getContentGooruId(),
              EventConstants.COLLECTION_RESOURCE_PLAY, EventConstants.STOP, false);
      if (allGraded == null || allGraded.isEmpty()) {
        sendCPStopEventToGEP(dcaReport);
        this.isGraded = true;
      } else {
        this.isGraded = false;
      }
      RDAEventDispatcher rdaEventDispatcher =
          new RDAEventDispatcher(dcaReport, this.views, this.reaction, (tsObj != null ? tsObj : 0),
              this.maxScore, this.score, this.isGraded, this.event.getEndTime());
      rdaEventDispatcher.sendCollectionStopDCAEventToRDA();
    }

    if ((event.getEventName().equals(EventConstants.COLLECTION_RESOURCE_PLAY)) && event
        .getEventType().equalsIgnoreCase(EventConstants.STOP)) {
      if (this.isGraded == true) {
        sendCRPEventToGEP(dcaReport);
      }
      RDAEventDispatcher rdaEventDispatcher = new RDAEventDispatcher(dcaReport, this.views,
          this.reaction, this.timespent, this.maxScore, this.score, this.isGraded,
          this.event.getEndTime());
      rdaEventDispatcher.sendCollectionResourcePlayDCAEventToRDA();
    }
    
    return new ExecutionResult<>(MessageResponseFactory.createCreatedResponse(),
        ExecutionStatus.SUCCESSFUL);
  }

  private void sendCPStopEventToGEP(AJEntityDailyClassActivity dcaReport) {  
    if (maxScoreObj != null && maxScoreObj > 0.0 && scoreObj != null) {
      GEPEventDispatcher eventDispatcher = new GEPEventDispatcher(dcaReport, (tsObj != null ? tsObj : 0),
          maxScoreObj, ((scoreObj * 100) / maxScoreObj), System.currentTimeMillis(), event.getAdditionalContext());
      eventDispatcher.sendCPStopEventFromDCAtoGEP();
    } else {
      GEPEventDispatcher eventDispatcher = new GEPEventDispatcher(dcaReport, (tsObj != null ? tsObj : 0),
          0.0, null, System.currentTimeMillis(), event.getAdditionalContext());
      eventDispatcher.sendCPStopEventFromDCAtoGEP();
    }    
  }

  private void sendCPStartEventToGEP(AJEntityDailyClassActivity dcaReport) {  
    GEPEventDispatcher eventDispatcher = new GEPEventDispatcher(dcaReport, 0L,
        null, null, System.currentTimeMillis(), event.getAdditionalContext());
    eventDispatcher.sendCPStartEventFromDCAtoGEP();
  }
  
  private void sendCRPEventToGEP(AJEntityDailyClassActivity dcaReport) { 
    
    JsonObject result = new JsonObject();
    
    if (event.getResourceType().equals(EventConstants.QUESTION)) {
      if (event.getAnswerStatus().equalsIgnoreCase(EventConstants.INCORRECT)
          || event.getAnswerStatus().equalsIgnoreCase(EventConstants.CORRECT)
          || event.getAnswerStatus().equalsIgnoreCase(EventConstants.SKIPPED)) {
        double sco = event.getScore();
        double max_sco = event.getMaxScore();
        if (max_sco > 0.0) {
          result.put(AJEntityDailyClassActivity.SCORE, ((sco * 100) / max_sco));
          result.put(AJEntityDailyClassActivity.MAX_SCORE, max_sco);
        } else {
          //TODO: Should the score be sent as NULL or 0.0
          result.putNull(AJEntityDailyClassActivity.SCORE);
          result.put(AJEntityDailyClassActivity.MAX_SCORE, 0.0);
        }
      } else if (event.getAnswerStatus().equalsIgnoreCase(EventConstants.ATTEMPTED)) {
        result.putNull(AJEntityDailyClassActivity.SCORE);
        result.put(AJEntityDailyClassActivity.MAX_SCORE, 0.0);
      }
    } else if (event.getResourceType().equals(EventConstants.RESOURCE)) {
      result.putNull(AJEntityDailyClassActivity.SCORE);
      result.putNull(AJEntityDailyClassActivity.MAX_SCORE);
    }

    result.put(AJEntityDailyClassActivity.TIMESPENT, event.getTimespent());
    result.put(AJEntityDailyClassActivity.RESOURCE_ATTEMPT_STATUS, event.getAnswerStatus());
        
    GEPEventDispatcher eventDispatcher = new GEPEventDispatcher(dcaReport, System.currentTimeMillis(), result);
    eventDispatcher.sendCRPEventFromDCAtoGEP();
  }
  
  @Override
  public boolean handlerReadOnly() {
    return false;
  }
}
