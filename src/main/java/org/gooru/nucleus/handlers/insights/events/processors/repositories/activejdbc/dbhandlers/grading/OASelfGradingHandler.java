package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.grading;

import java.sql.Timestamp;
import java.util.Map;
import java.util.UUID;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.gooru.nucleus.handlers.insights.events.processors.grading.GradingContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOACompletionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOASelfGrading;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOASubmissions;
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
public class OASelfGradingHandler implements DBHandler {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(OASelfGradingHandler.class);
  public static final String COLLECTION_TYPE = "collection_type";
  public static final String COLLECTION_ID = "collection_id";
  public static final String DCA_CONTENT_ID = "dca_content_id";
  private final GradingContext context;
  private String classId;
  private String oaId;
  private Long oaDcaId;
  private String studentId;
  private JsonObject req;
  private AJEntityOASelfGrading oaSelfGrading;
  private String contentSource;
  private String courseId;
  private String unitId;
  private String lessonId;
  private String collectionType;

  public OASelfGradingHandler(GradingContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {

    if (context.request() != null || !context.request().isEmpty()) {
      initializeRequestParams();
      if (collectionType == null || (collectionType != null && !collectionType.equalsIgnoreCase(EventConstants.OFFLINE_ACTIVITY))
          || StringUtil.isNullOrEmptyAfterTrim(contentSource) || !(contentSource!= null && EventConstants.CM_DCA_CONTENT_SOURCE.matcher(contentSource).matches())
          || !ValidationUtils.isValidUUID(classId) || !ValidationUtils.isValidUUID(oaId)
          || !ValidationUtils.isValidUUID(studentId)
          || StringUtil.isNullOrEmptyAfterTrim(contentSource)
          || (oaDcaId == null && contentSource.equalsIgnoreCase(EventConstants.DCA))
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

  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    // Student validation
    if (StringUtil.isNullOrEmpty(context.request().getString("userIdFromSession"))
        || (context.request().getString("userIdFromSession") != null
            && !context.request().getString("userIdFromSession").equals(studentId))) {
        return new
            ExecutionResult<>(MessageResponseFactory.createForbiddenResponse
            ("Auth Failure"), ExecutionStatus.FAILED);
    }
    AJEntityOACompletionStatus isOAMarkedComplete = null;
    if (contentSource.equalsIgnoreCase(EventConstants.DCA)) {
      isOAMarkedComplete = AJEntityOACompletionStatus.findFirst(
          AJEntityOACompletionStatus.GET_DCA_OA_MARKED_AS_COMPLETED, studentId, oaId, oaDcaId,
          classId, contentSource);
    } else if (contentSource.equalsIgnoreCase(EventConstants.COURSEMAP)) {
      isOAMarkedComplete = AJEntityOACompletionStatus.findFirst(
          AJEntityOACompletionStatus.GET_CM_OA_MARKED_AS_COMPLETED, studentId, oaId, classId,
          courseId, unitId, lessonId, contentSource);
    }
    if (isOAMarkedComplete == null) {
      return new ExecutionResult<>(
          MessageResponseFactory.createForbiddenResponse(
              "OA is not marked as complete. self grading is not possible before OA completion."),
          ExecutionStatus.FAILED);
    } 
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    oaSelfGrading = new AJEntityOASelfGrading();
    prune();

    new DefAJEntityOATaskSelfGradingEntityBuilder().build(oaSelfGrading, req,
        AJEntityOASelfGrading.getConverterRegistry());
    mapOAAttributes();

    if (!insertOrUpdateRecord(oaSelfGrading)) {
      return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
          ExecutionStatus.FAILED);    }

    LOGGER.debug("executeRequest() OK");
    return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(),
        ExecutionStatus.SUCCESSFUL);

  }


  private static class DefAJEntityOATaskSelfGradingEntityBuilder
      implements EntityBuilder<AJEntityOASelfGrading> {

  }

  private void initializeRequestParams() {
    req = context.request();
    classId = req.getString(AJEntityOASelfGrading.CLASS_ID);
    oaId = req.getString(COLLECTION_ID);
    oaDcaId = req.getLong(DCA_CONTENT_ID);
    studentId = req.getString(AJEntityOASelfGrading.STUDENT_ID);
    contentSource = req.getString(AJEntityOASubmissions.CONTENT_SOURCE);
    courseId = req.getString(EventConstants.COURSE_ID);
    unitId = req.getString(EventConstants.UNIT_ID);
    lessonId = req.getString(EventConstants.LESSON_ID);
    collectionType = req.getString(MessageConstants.COLLECTION_TYPE);
  }

  private void prune() {
    req.remove(COLLECTION_TYPE);
    req.remove(COLLECTION_ID);
    req.remove("userIdFromSession");
    if (req.containsKey(DCA_CONTENT_ID))
      req.remove(DCA_CONTENT_ID);
  }

  private void mapOAAttributes() {    
   oaSelfGrading.set(AJEntityOASelfGrading.OA_ID, UUID.fromString(oaId));
   if (contentSource.equalsIgnoreCase(EventConstants.DCA)) {
     oaSelfGrading.set(AJEntityOASelfGrading.OA_DCA_ID, oaDcaId);
   } else if (contentSource.equalsIgnoreCase(EventConstants.COURSEMAP)){
     oaSelfGrading.set(EventConstants.COURSE_ID, UUID.fromString(courseId));
     oaSelfGrading.set(EventConstants.UNIT_ID, UUID.fromString(unitId));
     oaSelfGrading.set(EventConstants.LESSON_ID, UUID.fromString(lessonId));
   }
  }
  private boolean insertOrUpdateRecord(AJEntityOASelfGrading oaSelfGrading) {
    AJEntityOASelfGrading duplicateRow = null;
    if (contentSource.equalsIgnoreCase(EventConstants.DCA)) {
      duplicateRow = AJEntityOASelfGrading.findFirst(
          "student_id = ?::uuid AND oa_id = ?::uuid AND oa_dca_id = ? AND class_id = ?::uuid AND content_source = ?",
          studentId, oaId, oaDcaId, classId, contentSource);
    } else if (contentSource.equalsIgnoreCase(EventConstants.COURSEMAP)) {
      duplicateRow = AJEntityOASelfGrading.findFirst(
          "student_id = ?::uuid AND oa_id = ?::uuid AND oa_dca_id IS NULL AND class_id = ?::uuid AND course_id = ?::uuid AND unit_id = ?::uuid AND lesson_id = ?::uuid AND content_source = ?",
          studentId, oaId, classId, courseId, unitId, lessonId, contentSource);
    }
    if (duplicateRow == null && oaSelfGrading.isValid()) {
      boolean result = oaSelfGrading.insert();
      if (!result) {
        LOGGER.error("Self Grades cannot be inserted for student {} & OA {} ", studentId, oaId);
        if (oaSelfGrading.hasErrors()) {
          Map<String, String> map = oaSelfGrading.errors();
          JsonObject errors = new JsonObject();
          map.forEach(errors::put);          
        }
        return false;
      } else {
        LOGGER.info("Self Grades inserted for student {} & OA {} ", studentId, oaId);
        return true;
      }
    } else if (duplicateRow != null && oaSelfGrading.isValid()){
      long id = Long.valueOf(duplicateRow.get("id").toString());
      //Timespent will not be inserted or updated from the self-grading flow, but will be updated
      //during the student submission
      int res = Base.exec(AJEntityOASelfGrading.UPDATE_OA_SELF_GRADE_FOR_THIS_STUDENT,
          oaSelfGrading.get(AJEntityOASelfGrading.RUBRIC_ID),
          oaSelfGrading.get(AJEntityOASelfGrading.GRADER),
          oaSelfGrading.get(AJEntityOASelfGrading.STUDENT_SCORE),
          oaSelfGrading.get(AJEntityOASelfGrading.MAX_SCORE),
          oaSelfGrading.get(AJEntityOASelfGrading.CATEGORY_SCORE),
          oaSelfGrading.get(AJEntityOASelfGrading.OVERALL_COMMENT),
          oaSelfGrading.get(AJEntityOASelfGrading.CONTENT_SOURCE),
          new Timestamp(System.currentTimeMillis()), id);
      if (res > 0) {
        LOGGER.info("Self Grades updated for student {} & OA {} ", studentId, oaId);
        return true;
      } else {
        LOGGER.error("Self Grades cannot be updated for student {} & OA {} ", studentId, oaId);
        return false;
      }  
    } else { //catchAll
      return false;
    }
  }

  @Override
  public boolean handlerReadOnly() {
    return false;
  }

}

