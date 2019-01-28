package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import com.hazelcast.util.StringUtil;
import io.vertx.core.json.JsonObject;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.gooru.nucleus.handlers.insights.events.bootstrap.EBSendVerticle;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.GEPConstants;
import org.gooru.nucleus.handlers.insights.events.processors.MessageDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.events.EventParser;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.DiagnosticEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.GradingPendingEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.LTIEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.RDAEventDispatcher;
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
 * Created by mukul@gooru Modified by daniel
 */
class ProcessEventHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessEventHandler.class);
  //TODO: This Kafka Topic name needs to be picked up from config
  public static final String TOPIC_GEP_USAGE_EVENTS = "org.gooru.da.sink.logW.usage.events";
  public static final String TOPIC_NOTIFICATIONS = "notifications";
  private final ProcessorContext context;
  private AJEntityReporting baseReport;
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

  public ProcessEventHandler(ProcessorContext context) {
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
    baseReport = new AJEntityReporting();
    boolean isGraded = true;
    event = context.getEvent();
    LazyList<AJEntityReporting> duplicateRow = null;
    LazyList<AJEntityReporting> scoreTS = null;
    LazyList<AJEntityReporting> allGraded = null;
    LazyList<AJEntityReporting> questions = null;

    baseReport.set("event_name", event.getEventName());
    baseReport.set("event_type", event.getEventType());
    baseReport.set("actor_id", event.getGooruUUID());
    baseReport.set("class_id", event.getClassGooruId());
    baseReport.set("course_id", event.getCourseGooruId());
    baseReport.set("unit_id", event.getUnitGooruId());
    baseReport.set("lesson_id", event.getLessonGooruId());
    baseReport.set("session_id", event.getSessionId());
    baseReport.set("collection_type", event.getCollectionType());
    baseReport.set("question_type", event.getQuestionType());
    baseReport.set("resource_type", event.getResourceType());
    baseReport.set("reaction", event.getReaction());

    baseReport.set("resource_attempt_status", event.getAnswerStatus());
    baseReport.set("views", event.getViews());
    baseReport.set("time_spent", event.getTimespent());
    baseReport.set("tenant_id", event.getTenantId());
    baseReport.set("max_score", event.getMaxScore());
    this.timespent = event.getTimespent();
    this.views = event.getViews();
    this.maxScore = event.getMaxScore();
    this.reaction = event.getReaction();

    baseReport.set("app_id", event.getAppId());
    baseReport.set("partner_id", event.getPartnerId());
    //pathId = 0L indicates the main Path. We store pathId only for the altPaths
    if (event.getPathId() != 0L) {
      baseReport.set("path_id", event.getPathId());
      if (!StringUtil.isNullOrEmpty(event.getPathType())) {
        if (EventConstants.PATH_TYPES.matcher(event.getPathType()).matches()) {
          baseReport.set("path_type", event.getPathType());
        } else {
          LOGGER.warn("Invalid Path Type is passed in event : {}", event.getPathType());
        }
      }
    }

    baseReport.set("created_at", new Timestamp(event.getStartTime()));
    baseReport.set("updated_at", new Timestamp(event.getEndTime()));

    baseReport.set("collection_sub_type", event.getCollectionSubType());
    baseReport.set("event_id", event.getEventId());
    baseReport.set("content_source", event.getContentSource());

    if (event.getTimeZone() != null) {
      String timeZone = event.getTimeZone();
      baseReport.set("time_zone", timeZone);
      String localeDate = UTCToLocale(event.getEndTime(), timeZone);

      if (localeDate != null) {
        baseReport.setDateinTZ(localeDate);
      }
    }

    if (event.getContextCollectionId() != null) {
      baseReport.set("context_collection_id", event.getContextCollectionId());
    }

    if (event.getContextCollectionType() != null) {
      baseReport.set("context_collection_type", event.getContextCollectionType());
    }

    if ((event.getEventName().equals(EventConstants.COLLECTION_PLAY))) {
      duplicateRow = AJEntityReporting
          .findBySQL(AJEntityReporting.CHECK_DUPLICATE_COLLECTION_EVENT, event.getGooruUUID(),
              event.getSessionId(),
              event.getContentGooruId(), event.getEventType(), event.getEventName());
      baseReport.set("collection_id", event.getContentGooruId());
      baseReport.set("question_count", event.getQuestionCount());
      if (event.getEventType().equalsIgnoreCase(EventConstants.START)) {
        baseReport.set("score", event.getScore());
        score = event.getScore();
      }

      if (event.getEventType().equalsIgnoreCase(EventConstants.STOP)) {
        if (event.getCollectionType().equalsIgnoreCase(EventConstants.EXTERNAL_ASSESSMENT) ||
            event.getCollectionType().equalsIgnoreCase(EventConstants.EXTERNAL_COLLECTION)) {
          double ms = event.getMaxScore();
          if (ms > 0.0) {
            baseReport.set("score", (event.getScore() * 100) / ms);
            score = (event.getScore() * 100) / ms;
          } else {
            baseReport.set("score", 0.0);
            score = 0.0;
          }
          baseReport.set("max_score", ms);
          baseReport.set("time_spent", event.getTimespent());
          maxScore = ms;
          timespent = event.getTimespent();
        } else {
          scoreTS = AJEntityReporting
              .findBySQL(AJEntityReporting.COMPUTE_ASSESSMENT_SCORE, event.getContentGooruId(),
                  event.getSessionId());
          if (!scoreTS.isEmpty()) {
            scoreTS.forEach(m -> {
              //If ALL Questions in Assessments are Free Response Questions, awaiting grading, score will be NULL
              scoreObj = (m.get(AJEntityReporting.SCORE) != null ?
                  Double.valueOf(m.get(AJEntityReporting.SCORE).toString()) : null);
              maxScoreObj = (m.get(AJEntityReporting.MAX_SCORE) != null ?
                  Double.valueOf(m.get(AJEntityReporting.MAX_SCORE).toString()) : null);
              tsObj = Long.valueOf(m.get(AJEntityReporting.TIMESPENT).toString());
            });

            //maxScore should be Null only in the case when all the questions in an Assessment are Free Response Question
            //In that case Score will not be calculated unless the questions are graded via the grading flow
            if (maxScoreObj != null && maxScoreObj > 0.0 && scoreObj != null) {
              baseReport.set("score", ((scoreObj * 100) / maxScoreObj));
              baseReport.set("max_score", maxScoreObj);
              maxScore = maxScoreObj;
              score = ((scoreObj * 100) / maxScoreObj);
            }
            if (event.getCollectionType().equalsIgnoreCase(EventConstants.ASSESSMENT)) {
              baseReport.set("time_spent", (tsObj != null ? tsObj : 0));
              timespent = (tsObj != null ? tsObj : 0);
            }
          }
        }
      }
    }

    if ((event.getEventName().equals(EventConstants.COLLECTION_RESOURCE_PLAY))) {
      duplicateRow = AJEntityReporting
          .findBySQL(AJEntityReporting.FIND_RESOURCE_EVENT, event.getGooruUUID(),
              event.getParentGooruId(),
              event.getSessionId(), event.getContentGooruId(), event.getEventType());
      baseReport.set("collection_id", event.getParentGooruId());
      baseReport.set("resource_id", event.getContentGooruId());
      baseReport.set("answer_object", event.getAnswerObject().toString());

      if (event.getResourceType().equals(EventConstants.QUESTION)) {
        if (event.getEventType().equalsIgnoreCase(EventConstants.START) && (event.getQuestionType()
            .equalsIgnoreCase(EventConstants.OE))) {
          baseReport.set("grading_type", event.getGradeType());
        } else if (event.getEventType().equalsIgnoreCase(EventConstants.START)) {
          baseReport.set("score", event.getScore());
          baseReport.setBoolean("is_graded", true);
          this.score = event.getScore();
          this.isGraded = true;
        }

        if (event.getEventType().equalsIgnoreCase(EventConstants.STOP) && (
            event.getAnswerStatus().equalsIgnoreCase(EventConstants.INCORRECT)
                || event.getAnswerStatus().equalsIgnoreCase(EventConstants.CORRECT)
                || event.getAnswerStatus().equalsIgnoreCase(EventConstants.SKIPPED))) {
          //Grading Type is set by default to "system", so no need to update the grading_type here.
          baseReport.set("score", event.getScore());
          baseReport.setBoolean("is_graded", true);
          this.score = event.getScore();
          isGraded = true;
          this.isGraded = true;
        } else if (event.getEventType().equalsIgnoreCase(EventConstants.STOP) &&
            (event.getAnswerStatus().equalsIgnoreCase(EventConstants.ATTEMPTED))) {
          baseReport.set("grading_type", event.getGradeType());
          baseReport.setBoolean("is_graded", false);
          isGraded = false;
          this.isGraded = false;
        }
      }
    }

    if ((event.getEventName().equals(EventConstants.REACTION_CREATE))) {
      baseReport.set("collection_id", event.getParentGooruId());
      baseReport.set("resource_id", event.getContentGooruId());
    }

    if (baseReport.hasErrors()) {
      LOGGER.warn("errors in creating Base Report");
    }
    LOGGER.debug("Inserting into Reports DB: " + context.request().toString());

    if (baseReport.isValid()) {
      if (duplicateRow == null || duplicateRow.isEmpty()) {
        if (baseReport.insert()) {
          LOGGER.info("Record inserted successfully in Reports DB");
        } else {
          LOGGER.error(
              "Error while inserting event into Reports DB: " + context.request().toString());
        }
      } else {
        LOGGER.debug("Found duplicate row in the DB, so updating duplicate row.....");
        if ((event.getEventName().equals(EventConstants.COLLECTION_RESOURCE_PLAY))) {
          duplicateRow.forEach(dup -> {
            int id = Integer.valueOf(dup.get("id").toString());
            long view = (Long.valueOf(dup.get("views").toString()) + event.getViews());
            long ts = (Long.valueOf(dup.get("time_spent").toString()) + event.getTimespent());
            long react = event.getReaction() != 0 ? event.getReaction() : 0;
            Double score = event.getScore();
            //update the Answer Object and Answer Status from the latest event
            //Rubrics - if the Answer Status is attempted then the default score that should be set is null
            if (event.getResourceType().equals(EventConstants.QUESTION) && event.getEventType()
                .equalsIgnoreCase(EventConstants.STOP)
                && event.getAnswerStatus().equalsIgnoreCase(EventConstants.ATTEMPTED)) {
              score = null;
            }
            Base.exec(AJEntityReporting.UPDATE_RESOURCE_EVENT, view, ts, score,
                new Timestamp(event.getEndTime()),
                react, event.getAnswerStatus(), event.getAnswerObject().toString(), id);
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
              if (event.getCollectionType().equalsIgnoreCase(EventConstants.EXTERNAL_ASSESSMENT) ||
                  event.getCollectionType().equalsIgnoreCase(EventConstants.EXTERNAL_COLLECTION)) {
                sco = 0.0;
                if (maxSco > 0.0) {
                  sco = ((event.getScore() * 100) / maxSco);
                }
              } else {
                //maxScore should be Null only in the case when all the questions in an Assessment are Free Response Question
                //In that case Score will not be calculated unless the questions are graded via the grading flow
                if (maxScoreObj != null && maxScoreObj > 0.0 && scoreObj != null) {
                  maxSco = maxScoreObj;
                  sco = (scoreObj * 100) / maxSco;
                } else {
                  sco = null;
                }
              }
            }
            Base.exec(AJEntityReporting.UPDATE_COLLECTION_EVENT, view, ts, sco, maxSco,
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
      sendCollStartEventtoGEP();
      RDAEventDispatcher rdaEventDispatcher = new RDAEventDispatcher(baseReport, this.views,
          this.reaction, this.timespent, this.maxScore, this.score, this.isGraded,
          this.event.getEndTime());
      rdaEventDispatcher.sendCollectionStartEventToRDA();
    }

    if ((event.getEventName().equals(EventConstants.COLLECTION_PLAY)) && event.getEventType()
        .equalsIgnoreCase(EventConstants.STOP)) {
      //Send Collection Performance Event to GEP only if ALL the questions have been GRADED
      allGraded = AJEntityReporting
          .findBySQL(AJEntityReporting.IS_COLLECTION_GRADED, event.getGooruUUID(),
              event.getSessionId(),
              event.getContentGooruId(), EventConstants.COLLECTION_RESOURCE_PLAY,
              EventConstants.STOP, false);
      if (allGraded == null || allGraded.isEmpty() 
    		  && !event.getContentSource().equalsIgnoreCase(EventConstants.COMPETENCY_MASTERY)) {
        sendCPEventtoGEP();
        this.isGraded = true;
      } else {
        this.isGraded = false;
        GradingPendingEventDispatcher eventDispatcher = new GradingPendingEventDispatcher(
            baseReport);
        eventDispatcher.sendGradingPendingEventtoNotifications();
      }
      RDAEventDispatcher rdaEventDispatcher = new RDAEventDispatcher(baseReport, this.views,
          this.reaction, (tsObj != null ? tsObj : 0), this.maxScore, this.score, this.isGraded,
          this.event.getEndTime());
      rdaEventDispatcher.sendCollectionStopEventToRDA();
      LTIEventDispatcher ltiEventDispatcher = new LTIEventDispatcher(baseReport, this.event,
          this.scoreObj, this.maxScore, this.score, this.isGraded);
      ltiEventDispatcher.sendCollPerfEventtoLTI();
      
      if (event.getContentSource().equalsIgnoreCase(EventConstants.DIAGNOSTIC)) {
          questions = AJEntityReporting
                  .findBySQL(AJEntityReporting.GET_DIAGNOSTIC_ASSESSMENT_QUESTIONS, event.getContentGooruId(),
                		  event.getSessionId(), event.getGooruUUID(), event.getClassGooruId()); 
          DiagnosticEventDispatcher diagnosticEventDispatcher = new DiagnosticEventDispatcher 
        		  (event.getContentGooruId(), event.getSessionId(), event.getGooruUUID(), 
        				  event.getClassGooruId(), score, toList(questions));
          diagnosticEventDispatcher.dispatchDiagnosticEvent();          
          }
      //The onus to ensure if this Assessment is a signature Item lies on the upstream systems.
      //Writer assumes that since the contentSource is "competencyMastery", assessment is a verified signature assessment,
      //& so if the score is >= 80% then the event should flow to DAP for skyline updation.
      if (event.getContentSource().equalsIgnoreCase(EventConstants.COMPETENCY_MASTERY) && score >= 80.00) {
    	  sendCPEventtoGEP();
      }
    }

    if ((event.getEventName().equals(EventConstants.COLLECTION_RESOURCE_PLAY)) && event
        .getEventType().equalsIgnoreCase(EventConstants.STOP) &&
        (isGraded == true)) {
      sendCRPEventtoGEP();
      RDAEventDispatcher rdaEventDispatcher = new RDAEventDispatcher(baseReport, this.views,
          this.reaction, this.timespent, this.maxScore, this.score, this.isGraded,
          this.event.getEndTime());
      rdaEventDispatcher.sendCollectionResourcePlayEventToRDA();
    }
    
    return new ExecutionResult<>(MessageResponseFactory.createCreatedResponse(),
        ExecutionStatus.SUCCESSFUL);

  }

  @Override
  public boolean handlerReadOnly() {
    return false;
  }

  //********************************************************************************************************

  private String UTCToLocale(Long strUtcDate, String timeZone) {

    String strLocaleDate = null;
    try {
      Long epohTime = strUtcDate;
      Date utcDate = new Date(epohTime);

      SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
      simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
      String strUTCDate = simpleDateFormat.format(utcDate);
      simpleDateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));

      strLocaleDate = simpleDateFormat.format(utcDate);

      LOGGER.debug("UTC Date String: " + strUTCDate);
      LOGGER.debug("Locale Date String: " + strLocaleDate);

    } catch (Exception e) {
      LOGGER.error(e.getMessage());
    }

    return strLocaleDate;
  }

  private List<String> toList (LazyList<AJEntityReporting> thisLazyList) {
	  List<String> thisList = new ArrayList<String>();
	  
	  for (AJEntityReporting l: thisLazyList) {
		  thisList.add(l.get(AJEntityReporting.RESOURCE_ID).toString());
	  }
	  return thisList; 
	  
  }
  //********************************************************
  //TODO: Move GEP Event Processing to the EventDispatcher 
  //********************************************************

  private void sendCPEventtoGEP() {
    JsonObject gepEvent = createCPEvent();
    JsonObject result = new JsonObject();

    if (event.getCollectionType().equalsIgnoreCase(EventConstants.EXTERNAL_ASSESSMENT) ||
        event.getCollectionType().equalsIgnoreCase(EventConstants.EXTERNAL_COLLECTION)) {
      double ms = event.getMaxScore();
      if (ms > 0.0) {
        result.put(GEPConstants.SCORE, (event.getScore() * 100) / ms);
      } else {
        result.put(GEPConstants.SCORE, 0.0);
      }
      result.put(GEPConstants.MAX_SCORE, ms);
      result.put(GEPConstants.TIMESPENT, event.getTimespent());

    } else {
      if (maxScoreObj != null && maxScoreObj > 0.0 && scoreObj != null) {
        result.put(GEPConstants.SCORE, ((scoreObj * 100) / maxScoreObj));
        result.put(GEPConstants.MAX_SCORE, maxScoreObj);
      } else {
        //TODO: Should the score be sent as NULL or 0.0
        result.putNull(GEPConstants.SCORE);
        result.put(GEPConstants.MAX_SCORE, 0.0);
      }
      result.put(GEPConstants.TIMESPENT, (tsObj != null ? tsObj : 0));
    }

    //This is for future Use. Currently no Reaction is associated the Assessment/Collection
    result.put(GEPConstants.REACTION, 0);
    gepEvent.put(GEPConstants.RESULT, result);

    try {
      LOGGER.debug("The Collection GEP Event is : {} ", gepEvent);
      MessageDispatcher.getInstance().sendGEPEvent2Kafka(TOPIC_GEP_USAGE_EVENTS, gepEvent);
      LOGGER.info("Successfully dispatched Collection Perf GEP Event..");
    } catch (Exception e) {
      LOGGER.error("Error while dispatching Collection Perf GEP Event ", e);
    }
  }

  private void sendCollStartEventtoGEP() {

    try {
      JsonObject gepEvent = createCollStartEvent();
      JsonObject result = new JsonObject();

      result.putNull(GEPConstants.SCORE);
      result.putNull(GEPConstants.MAX_SCORE);
      result.put(GEPConstants.TIMESPENT, 0.0);

      gepEvent.put(GEPConstants.RESULT, result);

      LOGGER.debug("The Collection Start GEP Event is : {} ", gepEvent);
      MessageDispatcher.getInstance().sendGEPEvent2Kafka(TOPIC_GEP_USAGE_EVENTS, gepEvent);
      LOGGER.info("Successfully dispatched Collection Start GEP Event..");
    } catch (Exception e) {
      LOGGER.error("Error while dispatching Collection Start GEP Event ", e);
    }
  }


  private void sendCRPEventtoGEP() {

    try {

      JsonObject gepEvent = createCRPEvent();
      JsonObject result = new JsonObject();

      //Currently there are no EVENTS generated for EXTERNAL_C/A.
//			if (event.getCollectionType().equalsIgnoreCase(EventConstants.EXTERNAL_ASSESSMENT) || 
//					event.getCollectionType().equalsIgnoreCase(EventConstants.EXTERNAL_COLLECTION)) {
//				
//			}

      if (event.getResourceType().equals(EventConstants.QUESTION)) {
        if (event.getAnswerStatus().equalsIgnoreCase(EventConstants.INCORRECT)
            || event.getAnswerStatus().equalsIgnoreCase(EventConstants.CORRECT)
            || event.getAnswerStatus().equalsIgnoreCase(EventConstants.SKIPPED)) {
          double sco = event.getScore();
          double max_sco = event.getMaxScore();
          if (max_sco > 0.0) {
            result.put(GEPConstants.SCORE, ((sco * 100) / max_sco));
            result.put(GEPConstants.MAX_SCORE, max_sco);
          } else {
            //TODO: Should the score be sent as NULL or 0.0
            result.putNull(GEPConstants.SCORE);
            result.put(GEPConstants.MAX_SCORE, 0.0);
          }
        } else if (event.getAnswerStatus().equalsIgnoreCase(EventConstants.ATTEMPTED)) {
          result.putNull(GEPConstants.SCORE);
          result.put(GEPConstants.MAX_SCORE, 0.0);
        }
      } else if (event.getResourceType().equals(EventConstants.RESOURCE)) {
        result.putNull(GEPConstants.SCORE);
        result.putNull(GEPConstants.MAX_SCORE);
      }

      result.put(GEPConstants.TIMESPENT, event.getTimespent());
      result.put(GEPConstants.ANSWER_STATUS, event.getAnswerStatus());
      gepEvent.put(GEPConstants.RESULT, result);

      LOGGER.debug("The Collection Resource GEP Event is : {} ", gepEvent);
      MessageDispatcher.getInstance().sendGEPEvent2Kafka(TOPIC_GEP_USAGE_EVENTS, gepEvent);
      LOGGER.info("Successfully dispatched Collection Resource GEP Event..");
    } catch (Exception e) {
      LOGGER.error("Error while dispatching Collection Resource GEP Event ", e);
    }
  }

  private JsonObject createCollStartEvent() {
    JsonObject cpEvent = new JsonObject();
    JsonObject context = new JsonObject();

    cpEvent.put(GEPConstants.USER_ID, event.getGooruUUID());
    cpEvent.put(GEPConstants.ACTIVITY_TIME, event.getEndTime());
    cpEvent.put(GEPConstants.EVENT_ID, event.getEventId());
    cpEvent.put(GEPConstants.EVENT_NAME, GEPConstants.COLLECTION_START_EVENT);
    cpEvent.put(GEPConstants.COLLECTION_ID, event.getContentGooruId());
    cpEvent.put(GEPConstants.COLLECTION_TYPE, event.getCollectionType());
    context.put(GEPConstants.CONTEXT_COLLECTION_ID, event.getContextCollectionId());
    context.put(GEPConstants.CONTEXT_COLLECTION_TYPE, event.getContextCollectionType());

    context.put(GEPConstants.CLASS_ID, event.getClassGooruId());
    context.put(GEPConstants.COURSE_ID, event.getCourseGooruId());
    context.put(GEPConstants.UNIT_ID, event.getUnitGooruId());
    context.put(GEPConstants.LESSON_ID, event.getLessonGooruId());
    context.put(GEPConstants.PATH_ID, event.getPathId());
    context.put(GEPConstants.SESSION_ID, event.getSessionId());
    context.put(GEPConstants.QUESTION_COUNT, event.getQuestionCount());
    context.put(GEPConstants.PARTNER_ID, event.getPartnerId());
    context.put(GEPConstants.TENANT_ID, event.getTenantId());
    context.put(GEPConstants.CONTENT_SOURCE, event.getContentSource());
    context.put(GEPConstants.PATH_TYPE, event.getPathType());

    cpEvent.put(GEPConstants.CONTEXT, context);

    return cpEvent;
  }

  private JsonObject createCPEvent() {
    JsonObject cpEvent = new JsonObject();
    JsonObject context = new JsonObject();

    cpEvent.put(GEPConstants.USER_ID, event.getGooruUUID());
    cpEvent.put(GEPConstants.ACTIVITY_TIME, event.getEndTime());
    cpEvent.put(GEPConstants.EVENT_ID, event.getEventId());
    cpEvent.put(GEPConstants.EVENT_NAME, GEPConstants.COLLECTION_PERF_EVENT);
    cpEvent.put(GEPConstants.COLLECTION_ID, event.getContentGooruId());
    cpEvent.put(GEPConstants.COLLECTION_TYPE, event.getCollectionType());
    context.put(GEPConstants.CONTEXT_COLLECTION_ID, event.getContextCollectionId());
    context.put(GEPConstants.CONTEXT_COLLECTION_TYPE, event.getContextCollectionType());

    context.put(GEPConstants.CLASS_ID, event.getClassGooruId());
    context.put(GEPConstants.COURSE_ID, event.getCourseGooruId());
    context.put(GEPConstants.UNIT_ID, event.getUnitGooruId());
    context.put(GEPConstants.LESSON_ID, event.getLessonGooruId());
    context.put(GEPConstants.PATH_ID, event.getPathId());
    context.put(GEPConstants.SESSION_ID, event.getSessionId());
    context.put(GEPConstants.QUESTION_COUNT, event.getQuestionCount());
    context.put(GEPConstants.PARTNER_ID, event.getPartnerId());
    context.put(GEPConstants.TENANT_ID, event.getTenantId());

    context.put(GEPConstants.CONTENT_SOURCE, event.getContentSource());
    context.put(GEPConstants.PATH_TYPE, event.getPathType());
    cpEvent.put(GEPConstants.CONTEXT, context);

    return cpEvent;
  }

  private JsonObject createCRPEvent() {
    JsonObject resEvent = new JsonObject();
    JsonObject context = new JsonObject();

    resEvent.put(GEPConstants.USER_ID, event.getGooruUUID());
    resEvent.put(GEPConstants.ACTIVITY_TIME, event.getEndTime());
    resEvent.put(GEPConstants.EVENT_ID, event.getEventId());
    resEvent.put(GEPConstants.EVENT_NAME, GEPConstants.RESOURCE_PERF_EVENT);
    resEvent.put(GEPConstants.RESOURCE_ID, event.getContentGooruId());
    resEvent.put(GEPConstants.RESOURCE_TYPE, event.getResourceType());

    context.put(GEPConstants.CLASS_ID, event.getClassGooruId());
    context.put(GEPConstants.COURSE_ID, event.getCourseGooruId());
    context.put(GEPConstants.UNIT_ID, event.getUnitGooruId());
    context.put(GEPConstants.LESSON_ID, event.getLessonGooruId());
    context.put(GEPConstants.COLLECTION_ID, event.getParentGooruId());
    context.put(GEPConstants.COLLECTION_TYPE, event.getCollectionType());
    context.put(GEPConstants.CONTEXT_COLLECTION_ID, event.getContextCollectionId());
    context.put(GEPConstants.CONTEXT_COLLECTION_TYPE, event.getContextCollectionType());
    context.put(GEPConstants.CONTENT_SOURCE, event.getContentSource());
    context.put(GEPConstants.PATH_ID, event.getPathId());
    context.put(GEPConstants.SESSION_ID, event.getSessionId());
    context.put(GEPConstants.PARTNER_ID, event.getPartnerId());
    context.put(GEPConstants.TENANT_ID, event.getTenantId());

    context.put(GEPConstants.PATH_TYPE, event.getPathType());
    resEvent.put(GEPConstants.CONTEXT, context);

    return resEvent;
  }
}
