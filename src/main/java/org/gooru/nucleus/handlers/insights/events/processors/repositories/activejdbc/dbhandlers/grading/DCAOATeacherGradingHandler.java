package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.grading;

import static org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.validators.ValidationUtils.*;
import java.sql.Timestamp;
import java.util.Map;
import java.util.UUID;
import org.gooru.nucleus.handlers.insights.events.constants.GEPConstants;
import org.gooru.nucleus.handlers.insights.events.processors.MessageDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.grading.GradingContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.RDAEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOASelfGrading;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityRubricGrading;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.EntityBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.util.StringUtil;
import io.vertx.core.json.JsonObject;


/**
 * @author mukul@gooru
 */
public class DCAOATeacherGradingHandler implements DBHandler {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(DCAOATeacherGradingHandler.class);
  public static final String TOPIC_GEP = "gep";
  public static final String OA_TYPE = "offline-activity";
  private AJEntityRubricGrading rubricGrading;
  private final GradingContext context;
  private JsonObject req;
  private String classId;
  private String studentId;
  private Long dcaContentId;
  private String collectionId;
  private Double score;
  private Double maxScore;
  
  public DCAOATeacherGradingHandler(GradingContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    
    if (context.request() != null || !context.request().isEmpty()) {
      initializeRequestParams();
      if (StringUtil.isNullOrEmpty(classId) || StringUtil.isNullOrEmpty(collectionId)
          || StringUtil.isNullOrEmpty(studentId) || (dcaContentId == null)) {
        LOGGER.warn("Invalid Json Payload");
        return new ExecutionResult<>(
            MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
            ExecutionStatus.FAILED);
      }
    } else {
      LOGGER.warn("Invalid Request Payload");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse("Invalid Request Payload"),
          ExecutionStatus.FAILED);
    }

    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  private void initializeRequestParams() {
    req = context.request();
    classId = req.getString(AJEntityRubricGrading.CLASS_ID);
    dcaContentId = req.getLong(AJEntityRubricGrading.DCA_CONTENT_ID);
    studentId = req.getString(AJEntityRubricGrading.STUDENT_ID);
    collectionId = req.getString(AJEntityRubricGrading.COLLECTION_ID);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    // Teacher validation
//    if (context.request().getString("userIdFromSession") != null) {
//      if (!context.request().getString("userIdFromSession")
//          .equals(studentId)) {
//        return new
//            ExecutionResult<>(MessageResponseFactory.createForbiddenResponse
//            ("Auth Failure"), ExecutionStatus.FAILED);
//      }
//    } else if (StringUtil.isNullOrEmpty(context.request().getString("userIdFromSession"))) {
//      return new
//          ExecutionResult<>(MessageResponseFactory.createForbiddenResponse
//          ("Auth Failure"), ExecutionStatus.FAILED);
//    }
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    rubricGrading = new AJEntityRubricGrading();
    score = req.getDouble(AJEntityRubricGrading.STUDENT_SCORE);
    maxScore = req.getDouble(AJEntityRubricGrading.MAX_SCORE);
    prune();
    
    new DefAJEntityDCAOATeacherGradingEntityBuilder()
    .build(rubricGrading, req, AJEntityRubricGrading.getConverterRegistry());
    
    if (!insertOrUpdateGradingRecord()) {
      return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
          ExecutionStatus.FAILED);    
      }

    //Teacher can only grade, once the OA Activity is marked as complete.So we should have OA record for this student/oaID
    //already present in Daily Class Activity table. So we only UPDATE that record here. If the record is not found, then
    //we should error out.
    if (!updateDCARecord()) {
      return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
          ExecutionStatus.FAILED);    
    }
    
    sendEventsToRDA();
    
    LOGGER.debug("executeRequest() OK");
    return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(),
        ExecutionStatus.SUCCESSFUL);

  }

  private static class DefAJEntityDCAOATeacherGradingEntityBuilder
      implements EntityBuilder<AJEntityRubricGrading> {

  }

  private void prune() {
    req.remove("userIdFromSession");
  }

  private boolean insertOrUpdateGradingRecord() {
    AJEntityRubricGrading duplicateRow = null;
    duplicateRow = AJEntityRubricGrading.findFirst("student_id = ? AND collection_id = ? AND dca_content_id = ? "
        + "AND class_id = ?", 
        studentId, collectionId, dcaContentId, classId);
    
    if (duplicateRow == null && rubricGrading.isValid()) {
      boolean result = rubricGrading.insert();
      if (!result) {
        LOGGER.error("Teacher Grades cannot be inserted for student {} & OA {} ", studentId, dcaContentId);
        if (rubricGrading.hasErrors()) {
          Map<String, String> map = rubricGrading.errors();
          JsonObject errors = new JsonObject();
          map.forEach(errors::put);          
        }
        return false;
      } else {
        LOGGER.info("Teacher Grades inserted for student {} & OA {} ", studentId, dcaContentId);
        sendInsertEventToGEP();
        return true;
      }
    } else if (duplicateRow != null && rubricGrading.isValid()){
      long id = Long.valueOf(duplicateRow.get("id").toString());
      int res = Base.exec(AJEntityRubricGrading.UPDATE_COLLECTION_GRADES,
          rubricGrading.get(AJEntityOASelfGrading.STUDENT_SCORE),
          rubricGrading.get(AJEntityOASelfGrading.MAX_SCORE),
          rubricGrading.get(AJEntityOASelfGrading.OVERALL_COMMENT),
          rubricGrading.get(AJEntityOASelfGrading.CATEGORY_SCORE),
          new Timestamp(System.currentTimeMillis()), id);
      if (res > 0) {
        LOGGER.info("Teacher Grades updated for student {} & OA {} ", studentId, dcaContentId);
        sendUpdateEventToGEP();
        return true;
      } else {
        LOGGER.error("Teacher Grades cannot be updated for student {} & OA {} ", studentId, dcaContentId);
        return false;
      }  
    } else { //catchAll
      return false;
    }
  }

  private boolean updateDCARecord() {
    if (rubricGrading.isValid()) {
      int result = 0;
      result = Base.exec(AJEntityDailyClassActivity.UPDATE_OA_SCORE, score, maxScore, true, studentId, collectionId, dcaContentId);
      LOGGER.debug("Total score updated successfully..."); 
      if (result > 0) {
        LOGGER.info("Score updated into DCA for student {} & OA {} ", studentId, dcaContentId);
        return true;
      } else {
        LOGGER.error("Score cannot be updated into DCA for student {} & OA {} ", studentId, dcaContentId);
        if (rubricGrading.hasErrors()) {
          Map<String, String> map = rubricGrading.errors();
          JsonObject errors = new JsonObject();
          map.forEach(errors::put);          
        }
        return false;
      }
    } else {
      LOGGER.error("Score cannot be updated into DCA for student {} & OA {} ", studentId, dcaContentId);
      return false;
    }
  }
  
  private void sendEventsToRDA() {
    String collectionType = rubricGrading.get(AJEntityRubricGrading.COLLECTION_TYPE) != null ? rubricGrading.get(AJEntityRubricGrading.COLLECTION_TYPE).toString() : OA_TYPE;
    LOGGER.info("Sending OA Teacher grade Event to RDA");
    RDAEventDispatcher rdaEventDispatcher = new RDAEventDispatcher(this.rubricGrading,
        collectionType, score, maxScore, null, null, null, null,
        true);
    rdaEventDispatcher.sendOATeacherGradeEventFromDCAOATGHToRDA();
  }
  
  private void sendInsertEventToGEP() {
    sendOATeacherGradingEventtoGEP(GEPConstants.COLLECTION_PERF_EVENT);
  }

  private void sendUpdateEventToGEP() {
    sendOATeacherGradingEventtoGEP(GEPConstants.COLL_SCORE_UPDATE_EVENT);
  }
  
  private void sendOATeacherGradingEventtoGEP(String eventName) {
    String cType = rubricGrading.get(AJEntityRubricGrading.COLLECTION_TYPE) != null ? rubricGrading.get(AJEntityRubricGrading.COLLECTION_TYPE).toString() : OA_TYPE;    
    LOGGER.info("Sending OA Teacher grade insert Event to GEP and RDA");

    JsonObject gepEvent = createOATeacherGradingEvent(cType, eventName);
    try {
      LOGGER.debug("The OA teacher grading GEP Event is : {} ", gepEvent);
      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_GEP, gepEvent);
      LOGGER.info("Successfully dispatched OA teacher grading GEP Event..");
    } catch (Exception e) {
      LOGGER.error("Error while dispatching OA teacher grading GEP Event ", e);
    }
  }

  private JsonObject createOATeacherGradingEvent(String collectionType, String eventName) {
    JsonObject cpEvent = new JsonObject();
    JsonObject context = new JsonObject();
    JsonObject result = new JsonObject();

    cpEvent.put(GEPConstants.USER_ID, rubricGrading.get(AJEntityRubricGrading.STUDENT_ID));
    cpEvent.put(GEPConstants.ACTIVITY_TIME, System.currentTimeMillis());
    cpEvent.put(GEPConstants.EVENT_ID, UUID.randomUUID().toString());
    cpEvent.put(GEPConstants.EVENT_NAME, eventName);
    cpEvent.put(GEPConstants.COLLECTION_ID, rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID));
    cpEvent.put(GEPConstants.COLLECTION_TYPE, collectionType);
    context.put(GEPConstants.CONTENT_SOURCE, GEPConstants.DAILY_CLASS_ACTIVIY);
    context.put(GEPConstants.CLASS_ID, rubricGrading.get(AJEntityRubricGrading.CLASS_ID));
    context.put(GEPConstants.COURSE_ID, rubricGrading.get(AJEntityRubricGrading.COURSE_ID));
    context.put(GEPConstants.UNIT_ID, rubricGrading.get(AJEntityRubricGrading.UNIT_ID));
    context.put(GEPConstants.LESSON_ID, rubricGrading.get(AJEntityRubricGrading.LESSON_ID));
    context.put(GEPConstants.SESSION_ID, rubricGrading.get(AJEntityRubricGrading.SESSION_ID));
    context.putNull(GEPConstants.ADDITIONAL_CONTEXT);

    cpEvent.put(GEPConstants.CONTEXT, context);

    if (validateScoreAndMaxScore(score, maxScore)) {
      result.put(GEPConstants.SCORE, rubricGrading.get(AJEntityRubricGrading.STUDENT_SCORE));
      result.put(GEPConstants.MAX_SCORE, rubricGrading.get(AJEntityRubricGrading.MAX_SCORE));
    } else {
      result.putNull(GEPConstants.SCORE);
      if (validateMaxScore(maxScore)) {
        result.put(GEPConstants.MAX_SCORE, rubricGrading.get(AJEntityRubricGrading.MAX_SCORE));
      } else {
        result.putNull(GEPConstants.MAX_SCORE);
      }
    }
    cpEvent.put(GEPConstants.RESULT, result);
    return cpEvent;
  }
  
  @Override
  public boolean handlerReadOnly() {
    return false;
  }

}

