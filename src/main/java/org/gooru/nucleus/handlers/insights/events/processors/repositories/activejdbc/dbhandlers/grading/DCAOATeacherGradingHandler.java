package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.grading;

import static org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.validators.ValidationUtils.validateScoreAndMaxScore;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.Map;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.GEPConstants;
import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.gooru.nucleus.handlers.insights.events.processors.grading.GradingContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.GEPEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.RDAEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOASelfGrading;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOASubmissions;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityRubricGrading;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.EntityBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.validators.ValidationUtils;
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
  private String contentSource;
  private String courseId;
  private String unitId;
  private String lessonId;
  private Long pathId;
  private String pathType;
  private String collectionType;
  
  public DCAOATeacherGradingHandler(GradingContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    
    if (context.request() != null || !context.request().isEmpty()) {
      initializeRequestParams();
      if (collectionType == null || (collectionType != null && !collectionType.equalsIgnoreCase(EventConstants.OFFLINE_ACTIVITY))
          || StringUtil.isNullOrEmptyAfterTrim(contentSource) || !(contentSource!= null && EventConstants.CM_DCA_CONTENT_SOURCE.matcher(contentSource).matches())
          || !ValidationUtils.isValidUUID(classId) || !ValidationUtils.isValidUUID(collectionId)
          || !ValidationUtils.isValidUUID(studentId)
          || (dcaContentId == null && contentSource.equalsIgnoreCase(EventConstants.DCA))
          || (contentSource.equalsIgnoreCase(EventConstants.COURSEMAP) && !(ValidationUtils.isValidUUID(courseId)
              && ValidationUtils.isValidUUID(unitId) && ValidationUtils.isValidUUID(lessonId)))) {
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
    collectionType = req.getString(MessageConstants.COLLECTION_TYPE);
    classId = req.getString(AJEntityRubricGrading.CLASS_ID);
    dcaContentId = req.getLong(AJEntityRubricGrading.DCA_CONTENT_ID);
    studentId = req.getString(AJEntityRubricGrading.STUDENT_ID);
    collectionId = req.getString(AJEntityRubricGrading.COLLECTION_ID);
    contentSource = req.getString(AJEntityOASubmissions.CONTENT_SOURCE);
    courseId = req.getString(EventConstants.COURSE_ID);
    unitId = req.getString(EventConstants.UNIT_ID);
    lessonId = req.getString(EventConstants.LESSON_ID);
    pathId = req.getLong(EventConstants._PATH_ID);
    pathType = req.getString(EventConstants._PATH_TYPE);
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
    if (contentSource.equalsIgnoreCase(EventConstants.DCA)) {
      if (!updateDCARecord()) {
        return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
            ExecutionStatus.FAILED);
      }
    } else if (contentSource.equalsIgnoreCase(EventConstants.COURSEMAP)) {
      if (!updateBaseReportsRecord()) {
        return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
            ExecutionStatus.FAILED);
      }
    }
    
    sendEventsToRDA();
    //Currently since SUGGESTIONS are not supported in the Grading Flow,
    //pathType & pathId are set to null and 0. Update pathType and pathId
    //when SUGGESTIONS are supported.
    sendOAScoreUpdateEventToGEP();
    LOGGER.debug("executeRequest() OK");
    return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(),
        ExecutionStatus.SUCCESSFUL);

  }

  private static class DefAJEntityDCAOATeacherGradingEntityBuilder
      implements EntityBuilder<AJEntityRubricGrading> {

  }

  private void prune() {
    req.remove("userIdFromSession");
    if (req.containsKey(AJEntityDailyClassActivity.PATH_ID)) req.remove(AJEntityDailyClassActivity.PATH_ID);
    if (req.containsKey(AJEntityDailyClassActivity.PATH_TYPE)) req.remove(AJEntityDailyClassActivity.PATH_TYPE);
  }

  private boolean insertOrUpdateGradingRecord() {
    AJEntityRubricGrading duplicateRow = null;
    if (contentSource.equalsIgnoreCase(EventConstants.DCA)) {
      duplicateRow = AJEntityRubricGrading.findFirst(
          "student_id = ? AND collection_id = ? AND dca_content_id = ? " + "AND class_id = ? AND content_source = ?",
          studentId, collectionId, dcaContentId, classId, contentSource);
    } else if (contentSource.equalsIgnoreCase(EventConstants.COURSEMAP)) {
      duplicateRow = AJEntityRubricGrading.findFirst(
          "student_id = ? AND collection_id = ? AND dca_content_id IS NULL "
              + "AND class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND content_source = ? ",
          studentId, collectionId, classId, courseId, unitId, lessonId, contentSource);
    }   
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
      Double scoreInPercent = null;
      if (validateScoreAndMaxScore(score, maxScore)) {
        scoreInPercent = ((score * 100) / maxScore);
        LOGGER.debug("Re-Computed total score {} ", scoreInPercent);
      }
      result = Base.exec(AJEntityDailyClassActivity.UPDATE_OA_SCORE, scoreInPercent, maxScore, true, studentId, collectionId, dcaContentId);
      if (result > 0) {
        LOGGER.info("Score updated into DCA for student {} & OA {} ", studentId, dcaContentId);
        return true;
      } else {
        LOGGER.error("Score cannot be updated into DCA for student {} & OA {} ", studentId, dcaContentId);
        return false;
      }
    } else {
      LOGGER.error("Score cannot be updated into DCA for student {} & OA {} ", studentId, dcaContentId);
      return false;
    }
  }
  
  private boolean updateBaseReportsRecord() {
    if (rubricGrading.isValid()) {
      int result = 0;
      Double scoreInPercent = null;
      if (validateScoreAndMaxScore(score, maxScore)) {
        scoreInPercent = ((score * 100) / maxScore);
        LOGGER.debug("Re-Computed total score {} ", scoreInPercent);
      }
      result = Base.exec(AJEntityReporting.UPDATE_OA_SCORE, scoreInPercent, maxScore, true, studentId, collectionId, courseId, unitId, lessonId, contentSource);
      if (result > 0) {
        LOGGER.info("Score updated into base reports for student {} & OA {} ", studentId, collectionId);
        return true;
      } else {
        LOGGER.error("Score cannot be updated into base reports for student {} & OA {} ", studentId, collectionId);
        return false;
      }
    } else {
      LOGGER.error("Score cannot be updated into base reports for student {} & OA {} ", studentId, collectionId);
      return false;
    }
  }
  
  private void sendEventsToRDA() {
    String collectionType = rubricGrading.get(AJEntityRubricGrading.COLLECTION_TYPE) != null ? rubricGrading.get(AJEntityRubricGrading.COLLECTION_TYPE).toString() : OA_TYPE;
    Double scoreInPercent = null;
    if (validateScoreAndMaxScore(score, maxScore)) {
      scoreInPercent = ((score * 100) / maxScore);
    }
    LOGGER.info("Sending OA Teacher grade Event to RDA");
    RDAEventDispatcher rdaEventDispatcher = new RDAEventDispatcher(this.rubricGrading,
        collectionType, scoreInPercent, maxScore, pathId, pathType, null, null,
        true);
    rdaEventDispatcher.sendOATeacherGradeEventFromOATGHToRDA();
  }

  private void sendOAScoreUpdateEventToGEP() {
    String additionalContext = null;
    if (contentSource.equalsIgnoreCase(EventConstants.DCA)) {
      additionalContext = setBase64EncodedAdditionalContext();
    }
    if (validateScoreAndMaxScore(score, maxScore))  {      
      GEPEventDispatcher eventDispatcher = new GEPEventDispatcher(rubricGrading, 
          maxScore, ((score * 100) / maxScore), System.currentTimeMillis(), additionalContext, pathId, pathType);
      eventDispatcher.sendScoreUpdateEventFromOATGHtoGEP();
    } else {
      GEPEventDispatcher eventDispatcher = new GEPEventDispatcher(rubricGrading,
          0.0, null, System.currentTimeMillis(), additionalContext, pathId, pathType);
      eventDispatcher.sendScoreUpdateEventFromOATGHtoGEP();
    }    
  }

  private String setBase64EncodedAdditionalContext() {
    String base64encodedString;
    try {
      JsonObject additionalContext = new JsonObject();
      additionalContext.put(GEPConstants.DCA_CONTENT_ID, dcaContentId);
      base64encodedString = Base64.getEncoder().encodeToString(
          additionalContext.toString().getBytes(GEPConstants.UTF8));
    } catch (UnsupportedEncodingException e) {
      LOGGER.error("Error while encoding additionalContext", e);
      return null;
    }
    return base64encodedString;
  }
  
  @Override
  public boolean handlerReadOnly() {
    return false;
  }

}

