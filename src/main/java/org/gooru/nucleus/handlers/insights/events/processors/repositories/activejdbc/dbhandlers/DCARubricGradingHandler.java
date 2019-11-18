package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.GEPConstants;
import org.gooru.nucleus.handlers.insights.events.processors.MessageDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.RDAEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.RubricGradingEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
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
import com.hazelcast.util.StringUtil;
import io.vertx.core.json.JsonObject;

/**
 * @author mukul@gooru
 */
public class DCARubricGradingHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(DCARubricGradingHandler.class);
  // TODO: This Kafka Topic name needs to be picked up from config
  public static final String TOPIC_GEP_USAGE_EVENTS = "org.gooru.da.sink.logW.usage.events";
  //public static final String TOPIC_NOTIFICATIONS = "notifications";
  public static final String ADDITIONAL_CONTEXT = "additional_context";
  private final ProcessorContext context;
  private AJEntityRubricGrading rubricGrading;
  private Double score;
  private Double rawScore;
  private Double max_score;
  private Long pathId;
  private String latestSessionId;
  private String pathType;
  private String contextCollectionId;
  private String contextCollectionType;
  private Boolean isGraded;
  private String additionalContext = null;
  private String additionalContextEncoded = null;
  private Date activityDate;
//  private String updated_at;
//  private String tenantId;
//  private String partnerId;
//  private Integer queCount = 0;

  public DCARubricGradingHandler(ProcessorContext context) {
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
      List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER,
          context.request().getString("class_id"),
          context.request().getString("userIdFromSession"));
      if (owner.isEmpty()) {
        LOGGER.warn("User is not a teacher or collaborator");
      }
    }
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    rubricGrading = new AJEntityRubricGrading();
    LazyList<AJEntityDailyClassActivity> allGraded = null;
    JsonObject req = context.request();
    String teacherId = req.getString("userIdFromSession");
    req.remove("userIdFromSession");
    //Analytics will not store gut_codes in the Rubric Grading Table.
    req.remove(AJEntityRubricGrading.GUT_CODES);
    //TODO: Other than passing this additional_context to the DAP, 
    //we need to decide if this also needs to be stored at the rubrics table
    additionalContextEncoded = req.getString(ADDITIONAL_CONTEXT);
    req.remove(ADDITIONAL_CONTEXT);
    //TODO * activity_date maybe redundant post dca_content_id is posted by FE
    String date = req.getString("activity_date");
    if (!StringUtil.isNullOrEmpty(date)) {
      this.activityDate = Date.valueOf(date);
    }
    //We will not store activity_date in the Rubrics Table as of now.
    //Based on the offline design this may change.  
    req.remove("activity_date");
    LazyList<AJEntityRubricGrading> duplicateRow = null;

    new DefAJEntityDCARubricGradingEntityBuilder()
    .build(rubricGrading, req, AJEntityRubricGrading.getConverterRegistry());
    rubricGrading.set(AJEntityRubricGrading.CONTENT_SOURCE,
        req.getString(AJEntityRubricGrading.CONTENT_SOURCE, null) != null
            ? req.getString(AJEntityRubricGrading.CONTENT_SOURCE)
            : EventConstants.DCA);
    String collType = rubricGrading.getString(AJEntityRubricGrading.COLLECTION_TYPE);
    LOGGER.debug("Collection Type " + collType);
    Object sessionId = rubricGrading.get(AJEntityRubricGrading.SESSION_ID);
    if (sessionId == null) {
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse("Invalid SessionId"),
          ExecutionResult.ExecutionStatus.FAILED);
    }

    //Going ahead Teacher will be able to update the FR that is previously graded, so then this flow
    //becomes more critical. 
    //With the current implementation we may not encounter dup events. 
    //(& even if we do, its assumed that Rubrics associated with the Question will not 
    //so only score/max_score will get updated)
    duplicateRow =
        AJEntityRubricGrading.findBySQL(AJEntityRubricGrading.THIS_QUESTION_DCA_GRADES_FIND,
            rubricGrading.get(AJEntityRubricGrading.STUDENT_ID),
            rubricGrading.get(AJEntityRubricGrading.CLASS_ID),
            rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID),
            rubricGrading.get(AJEntityRubricGrading.RESOURCE_ID),
            rubricGrading.get(AJEntityRubricGrading.SESSION_ID));

    if (duplicateRow == null || duplicateRow.isEmpty()) {
      rubricGrading.set(AJEntityRubricGrading.GRADER_ID, teacherId);
      rubricGrading.set("grader", "Teacher");
      //Timestamps are mandatory
      rubricGrading.set("created_at", new Timestamp(
          Long.valueOf(rubricGrading.get(AJEntityRubricGrading.CREATED_AT).toString())));
      rubricGrading.set("updated_at", new Timestamp(
          Long.valueOf(rubricGrading.get(AJEntityRubricGrading.UPDATED_AT).toString())));

      boolean result = rubricGrading.save();
      if (!result) {
        LOGGER.error("Grades cannot be inserted into the DB for the student " + req
            .getValue(AJEntityRubricGrading.STUDENT_ID));

        if (rubricGrading.hasErrors()) {
          Map<String, String> map = rubricGrading.errors();
          JsonObject errors = new JsonObject();
          map.forEach(errors::put);
          return new ExecutionResult<>(MessageResponseFactory.createValidationErrorResponse(errors),
              ExecutionResult.ExecutionStatus.FAILED);
        }
      }
      LOGGER.debug("Student Rubric grades stored successfully for the student " + req.getValue(AJEntityRubricGrading.STUDENT_ID));
    } else if (duplicateRow != null) {
      LOGGER.debug("Found duplicate row in the DB, this question appears to be graded previously, will be updated");
      duplicateRow.forEach(dup -> {
        long id = Long.valueOf(dup.get("id").toString());
        //update the Answer Object and Answer Status from the latest event
        Base.exec(AJEntityRubricGrading.UPDATE_QUESTION_GRADES,
            rubricGrading.get(AJEntityRubricGrading.STUDENT_SCORE),
            rubricGrading.get(AJEntityRubricGrading.MAX_SCORE),
            rubricGrading.get(AJEntityRubricGrading.OVERALL_COMMENT),
            rubricGrading.get(AJEntityRubricGrading.CATEGORY_SCORE),
            new Timestamp(
                Long.valueOf(rubricGrading.get(AJEntityRubricGrading.UPDATED_AT).toString())), id);
      });
    }

    if (sessionId != null) {
      LOGGER.debug("Student Score : {} ",
          rubricGrading.get(AJEntityRubricGrading.STUDENT_SCORE));
      LOGGER.debug("Max Score : {} ", rubricGrading.get(AJEntityRubricGrading.MAX_SCORE));
      LOGGER.debug("session id : {} ", sessionId.toString());
      LOGGER.debug("resource id : {} ", rubricGrading.get(AJEntityRubricGrading.RESOURCE_ID));

      Base.exec(AJEntityDailyClassActivity.UPDATE_QUESTION_SCORE,
          rubricGrading.get(AJEntityRubricGrading.STUDENT_SCORE),
          rubricGrading.get(AJEntityRubricGrading.MAX_SCORE), true, rubricGrading.get(AJEntityRubricGrading.STUDENT_ID),
          rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID), sessionId.toString(),
          rubricGrading.get(AJEntityRubricGrading.RESOURCE_ID));

      LOGGER.debug("Computing total score...");
      LazyList<AJEntityDailyClassActivity> scoreTS = AJEntityDailyClassActivity
          .findBySQL(AJEntityDailyClassActivity.COMPUTE_ASSESSMENT_SCORE_POST_GRADING,
              rubricGrading.get(AJEntityRubricGrading.STUDENT_ID), rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID), 
              sessionId.toString());
      //LOGGER.debug("scoreTS {} ", scoreTS);

      if (scoreTS != null && !scoreTS.isEmpty()) {
        scoreTS.forEach(m -> {
          rawScore = (m.get(AJEntityDailyClassActivity.SCORE) != null ? Double
              .valueOf(m.get(AJEntityDailyClassActivity.SCORE).toString()) : null);
          LOGGER.debug("rawScore {} ", rawScore);
          max_score = (m.get(AJEntityDailyClassActivity.MAX_SCORE) != null ? Double
              .valueOf(m.get(AJEntityDailyClassActivity.MAX_SCORE).toString()) : null);
          LOGGER.debug("max_score {} ", max_score);
        });

        if (rawScore != null && max_score != null && max_score > 0.0) {
          score = ((rawScore * 100) / max_score);
          LOGGER.debug("Re-Computed total score {} ", score);
        }
        Base.exec(AJEntityDailyClassActivity.UPDATE_ASSESSMENT_SCORE, score, max_score, rubricGrading.get(AJEntityRubricGrading.STUDENT_ID),
            rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID), sessionId.toString());
        LOGGER.debug("Total score updated successfully...");
      } else {
        LOGGER.error("Total score cannot be computed...");
      }
    }

    AJEntityDailyClassActivity sessionPathIdTypeModel = AJEntityDailyClassActivity
        .findFirst("actor_id = ? AND class_id = ? AND collection_id = ? AND "
            + "event_name = 'collection.play' AND event_type = 'stop' ORDER BY updated_at DESC",
            rubricGrading.get(AJEntityRubricGrading.STUDENT_ID),
            rubricGrading.get(AJEntityRubricGrading.CLASS_ID),
            rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID));
    if (sessionPathIdTypeModel != null) {
      latestSessionId = sessionPathIdTypeModel.get(AJEntityDailyClassActivity.SESSION_ID) != null ?
              sessionPathIdTypeModel.get(AJEntityDailyClassActivity.SESSION_ID).toString() : null;
      // since DCA doesn't have suggestions, pathType and pathId is not applicable for DCA (as of now),
      // so the values should always be null and 0L respectively              
      pathType = sessionPathIdTypeModel.get(AJEntityDailyClassActivity.PATH_TYPE) != null
          ? sessionPathIdTypeModel.get(AJEntityDailyClassActivity.PATH_TYPE).toString() : null;
      pathId = sessionPathIdTypeModel.get(AJEntityDailyClassActivity.PATH_ID) != null
          ? Long.valueOf(sessionPathIdTypeModel.get(AJEntityDailyClassActivity.PATH_ID).toString()) : 0L;
      contextCollectionId = sessionPathIdTypeModel.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_ID) != null
              ? sessionPathIdTypeModel.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_ID).toString() : null;
      contextCollectionType = sessionPathIdTypeModel.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_TYPE) != null
              ? sessionPathIdTypeModel.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_TYPE).toString() : null;
//      updated_at = sessionPathIdTypeModel.get(AJEntityDailyClassActivity.UPDATE_TIMESTAMP).toString();
//      partnerId = sessionPathIdTypeModel.get(AJEntityDailyClassActivity.PARTNER_ID) != null
//          ? sessionPathIdTypeModel.get(AJEntityDailyClassActivity.PARTNER_ID).toString() : null;
//      tenantId = sessionPathIdTypeModel.get(AJEntityDailyClassActivity.TENANT_ID) != null
//          ? sessionPathIdTypeModel.get(AJEntityDailyClassActivity.TENANT_ID).toString() : null;
//      queCount = sessionPathIdTypeModel.get(AJEntityReporting.QUESTION_COUNT) != null ? Integer.valueOf(sessionPathIdTypeModel
//                  .get(AJEntityReporting.QUESTION_COUNT).toString()) : 0;              
    }

    if ((!StringUtil.isNullOrEmpty(latestSessionId) &&
        latestSessionId.equals(sessionId.toString()))
        && (rubricGrading.get(AJEntityRubricGrading.STUDENT_SCORE) != null)) {
      LOGGER.info("Sending Resource Update Score Event to GEP");
      sendResourceScoreUpdateEventtoGEP(collType.toString());
      //Note that {"score":0 & "maxScore":0} is possible for Questions that have Rubrics with Scoring OFF.
      //In this case, teacher will update only comments & no scores.
      //NOTIFICATIONS EVENTS for No Scoring events should be generated from here.
//      if (queCount == 1 && !checkNoScoreAndMaxScore()) {
//        LazyList<AJEntityReporting>  queGraded = AJEntityReporting.findBySQL(AJEntityReporting.IS_COLLECTION_GRADED,
//            rubricGrading.get(AJEntityRubricGrading.STUDENT_ID),
//            latestSessionId, rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID),
//            EventConstants.COLLECTION_RESOURCE_PLAY, EventConstants.STOP, false);
//        if (queGraded == null || queGraded.isEmpty()) {
//          RubricGradingEventDispatcher eventDispatcher = new RubricGradingEventDispatcher(
//              rubricGrading, pathType, pathId, contextCollectionId, contextCollectionType);
//          eventDispatcher.sendGradingCompleteTeacherEventtoNotifications();
//          eventDispatcher.sendGradingCompleteStudentEventtoNotifications();
//          RDAEventDispatcher rdaEventDispatcher = new RDAEventDispatcher(this.rubricGrading,
//              collType.toString(), 0.0, 0.0, pathId, pathType, contextCollectionId, contextCollectionType,
//              true);
//          rdaEventDispatcher.sendCollScoreUpdateEventFromRGHToRDA();
//        }      
//      }
    } else {
      LOGGER.info("The latest Session Id for this activity is {} and not {}. "
          + "Events will not be propogated to the downstream systems for this grading actvity ", latestSessionId, sessionId.toString());
    }

    if ((!StringUtil.isNullOrEmpty(latestSessionId) &&
        latestSessionId.equals(sessionId.toString()))
        && score != null && max_score != null) {
      LOGGER.info("Sending Collection Update Score Event to GEP, RDA");
      allGraded =
          AJEntityDailyClassActivity.findBySQL(AJEntityDailyClassActivity.IS_COLLECTION_GRADED,
              rubricGrading.get(AJEntityRubricGrading.STUDENT_ID),
              latestSessionId, rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID),
              EventConstants.COLLECTION_RESOURCE_PLAY, EventConstants.STOP, false);
      isGraded = false;
      if (allGraded == null || allGraded.isEmpty()) {
        sendCollScoreUpdateEventtoGEP(collType.toString());
        isGraded = true;
        //TODO: Uncomment this for notifications
        // Currently we are not going to send NOTIFICATION EVENTS for GRADING FROM DCA.        
        // RubricGradingEventDispatcher eventDispatcher = new RubricGradingEventDispatcher(
        // rubricGrading, pathType, pathId, contextCollectionId, contextCollectionType);
        // eventDispatcher.sendGradingCompleteTeacherEventtoNotifications();
        // eventDispatcher.sendGradingCompleteStudentEventtoNotifications();
      }

      //In the current scenario, pathId and pathType will be 0L and null respectively for DCA
      RDAEventDispatcher rdaEventDispatcher = new RDAEventDispatcher(this.rubricGrading,
          collType.toString(), score, max_score, pathId, pathType, contextCollectionId,
          contextCollectionType,
          isGraded);      
      rdaEventDispatcher.sendCollScoreUpdateEventFromRGHToRDA();
    }

    return new ExecutionResult<>(MessageResponseFactory.createCreatedResponse(),
        ExecutionStatus.SUCCESSFUL);
  }

  private static class DefAJEntityDCARubricGradingEntityBuilder
  implements EntityBuilder<AJEntityRubricGrading> {

  }
  
  @Override
  public boolean handlerReadOnly() {
    return false;
  }

  
  private boolean checkNoScoreAndMaxScore() {
    Double sco = rubricGrading.get(AJEntityRubricGrading.STUDENT_SCORE) != null ? 
        Double.valueOf(rubricGrading.get(AJEntityRubricGrading.STUDENT_SCORE).toString()) : null;
    Double maxSco = rubricGrading.get(AJEntityRubricGrading.MAX_SCORE) != null ? 
        Double.valueOf(rubricGrading.get(AJEntityRubricGrading.MAX_SCORE).toString()) : null;    
    return !(sco == null || maxSco == null || ((sco.compareTo(0.00) == 0)
        && (maxSco.compareTo(0.00) == 0)));
  }

  //TODO
  private void pruneRequest() {
    
  }
    
  private void decodeAdditionalContext() {
    try {
      additionalContext = new String(Base64.getDecoder().decode(additionalContextEncoded));
      LOGGER.info("Decoded Additional Context is {}", additionalContext);       
    } catch (IllegalArgumentException e) {
      LOGGER.error("Unable to decode Additional Context ", e);
  }  
  }
  
  // ********************************************************
  // TODO: Move GEP Event Processing to the EventDispatcher
  // ********************************************************

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
    context.put(GEPConstants.COLLECTION_ID,
        rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID));
    context.put(GEPConstants.COLLECTION_TYPE, collectionType);

    context.put(GEPConstants.CONTENT_SOURCE, GEPConstants.DAILY_CLASS_ACTIVIY);
    context.put(GEPConstants.CONTEXT_COLLECTION_ID, contextCollectionId);
    context.put(GEPConstants.CONTEXT_COLLECTION_TYPE, contextCollectionType);
    context.put(GEPConstants.PATH_ID, pathId);
    context.put(GEPConstants.PATH_TYPE, pathType);
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
    cpEvent.put(GEPConstants.COLLECTION_ID,
        rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID));
    cpEvent.put(GEPConstants.COLLECTION_TYPE, collectionType);

    context.put(GEPConstants.CONTEXT_COLLECTION_ID, contextCollectionId);
    context.put(GEPConstants.CONTEXT_COLLECTION_TYPE, contextCollectionType);
    context.put(GEPConstants.PATH_ID, pathId);
    context.put(GEPConstants.PATH_TYPE, pathType);

    context.put(GEPConstants.CONTENT_SOURCE, GEPConstants.DAILY_CLASS_ACTIVIY);
    context.put(GEPConstants.CLASS_ID, rubricGrading.get(AJEntityRubricGrading.CLASS_ID));
    context.put(GEPConstants.COURSE_ID, rubricGrading.get(AJEntityRubricGrading.COURSE_ID));
    context.put(GEPConstants.UNIT_ID, rubricGrading.get(AJEntityRubricGrading.UNIT_ID));
    context.put(GEPConstants.LESSON_ID, rubricGrading.get(AJEntityRubricGrading.LESSON_ID));
    context.put(GEPConstants.SESSION_ID, rubricGrading.get(AJEntityRubricGrading.SESSION_ID));
    context.put(GEPConstants.ADDITIONAL_CONTEXT, additionalContext);

    cpEvent.put(GEPConstants.CONTEXT, context);

    if (score != null && max_score != null && max_score > 0.0) {
      // score = ((score * 100) / max_score);
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
