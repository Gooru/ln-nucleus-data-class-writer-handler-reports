package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.oa;

import java.sql.Timestamp;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.gooru.nucleus.handlers.insights.events.processors.oa.OAContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOACompletionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOASelfGrading;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOASubmissions;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.validators.ValidationUtils;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.util.StringUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


/**
 * @author mukul@gooru
 */
public class OASubmissionsHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(OASubmissionsHandler.class);
  private static final String TIMESPENT = "time_spent";
  private final OAContext context;
  private String classId;
  private String oaId;
  private Long oaDcaId;
  private String studentId;
  private Long timeSpent;
  private JsonObject req;
  private JsonArray submissions;
  private Pattern SUBMISSION_TYPES = Pattern.compile("uploaded|remote|free-form-text");
  private String FREE_FORM_TEXT = "free-form-text";
  private String contentSource;
  private String courseId;
  private String unitId;
  private String lessonId;

  public OASubmissionsHandler(OAContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    req = context.request();
    classId = context.request().getString(AJEntityOASubmissions.CLASS_ID);
    oaId = context.request().getString(AJEntityOASubmissions.OA_ID);
    oaDcaId = context.request().getLong(AJEntityOASubmissions.OA_DCA_ID);
    studentId = context.request().getString(AJEntityOASubmissions.STUDENT_ID);
    contentSource = context.request().getString(AJEntityOASubmissions.CONTENT_SOURCE);
    courseId = context.request().getString(EventConstants.COURSE_ID);
    unitId = context.request().getString(EventConstants.UNIT_ID);
    lessonId = context.request().getString(EventConstants.LESSON_ID);

    if (context.request() != null || !context.request().isEmpty()) {
      if (!ValidationUtils.isValidUUID(classId) || !ValidationUtils.isValidUUID(oaId)
          || !ValidationUtils.isValidUUID(studentId)
          || StringUtil.isNullOrEmptyAfterTrim(contentSource)
          || (oaDcaId == null && contentSource.equalsIgnoreCase(EventConstants.DCA))
          || !(EventConstants.CM_DCA_CONTENT_SOURCE.matcher(contentSource).matches())
          || (contentSource.equalsIgnoreCase(EventConstants.COURSEMAP)
              && !(ValidationUtils.isValidUUID(courseId) && ValidationUtils.isValidUUID(unitId)
                  && ValidationUtils.isValidUUID(lessonId)))) {
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
    
    if (req.getJsonArray(MessageConstants.SUBMISSIONS) != null
        && !req.getJsonArray(MessageConstants.SUBMISSIONS).isEmpty()) {
      submissions = req.getJsonArray(MessageConstants.SUBMISSIONS);
      for (Object sub : submissions) {
        JsonObject submission = (JsonObject) sub;
        String submissionType = submission.getString(AJEntityOASubmissions.SUBMISSION_TYPE); 
        if (submissionType == null || !SUBMISSION_TYPES.matcher(submissionType).matches()) {
          return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse("Invalid Submission Type in Payload"),
              ExecutionStatus.FAILED);
        }
      }
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
        return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("Auth Failure"),
            ExecutionStatus.FAILED);
    }
    AJEntityOACompletionStatus isOAMarkedComplete = null;
    if (contentSource.equalsIgnoreCase(EventConstants.DCA)) {
      isOAMarkedComplete = AJEntityOACompletionStatus.findFirst(
          AJEntityOACompletionStatus.GET_DCA_OA_MARKED_AS_COMPLETED, studentId, oaId, oaDcaId, classId,
          contentSource);
    } else if (contentSource.equalsIgnoreCase(EventConstants.COURSEMAP)) {
      isOAMarkedComplete = AJEntityOACompletionStatus.findFirst(
          AJEntityOACompletionStatus.GET_CM_OA_MARKED_AS_COMPLETED, studentId, oaId, classId,
          courseId, unitId, lessonId, contentSource);
    }
    if (isOAMarkedComplete != null) {
      return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse(
          "OA is marked as completed. Submission is closed."), ExecutionStatus.FAILED);
    }
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    timeSpent = context.request().getLong(TIMESPENT);

    if (submissions != null) {
      for (Object sub : submissions) {
        AJEntityOASubmissions oaSubmissions = setOASubmissionsModel();
        JsonObject submission = (JsonObject) sub;
        oaSubmissions.set(AJEntityOASubmissions.TASK_ID,
            submission.getLong(AJEntityOASubmissions.TASK_ID));
        String subInfo = submission.getString(AJEntityOASubmissions.SUBMISSION_INFO);
        if (!StringUtil.isNullOrEmpty(subInfo)) {
          String submissionType = submission.getString(AJEntityOASubmissions.SUBMISSION_TYPE);

          oaSubmissions.set(AJEntityOASubmissions.SUBMISSION_INFO, subInfo);
          oaSubmissions.set(AJEntityOASubmissions.SUBMISSION_TYPE,
              submission.getString(AJEntityOASubmissions.SUBMISSION_TYPE));
          oaSubmissions.set(AJEntityOASubmissions.SUBMISSION_SUBTYPE,
              submission.getString(AJEntityOASubmissions.SUBMISSION_SUBTYPE));
          // Auto-update ID
          oaSubmissions.set(AJEntityDailyClassActivity.ID, null);
          if (submissionType.equalsIgnoreCase(FREE_FORM_TEXT)) {
            if (!insertOrUpdateSubmissionText(oaSubmissions)) {
              return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
                  ExecutionStatus.FAILED);
            }
          } else {
            if (!insertRecord(oaSubmissions)) {
              return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
                  ExecutionStatus.FAILED);
            }
          }
        }
      }
    }

    //Timespent will be updated by the student during the submissions & not during self-grading.
    if (timeSpent != null) {
      if (!insertOrUpdateTS()) {
        return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
            ExecutionStatus.FAILED);        
      }
    }
    
    LOGGER.debug("executeRequest() OK");
    return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(),
        ExecutionStatus.SUCCESSFUL);

  }

  private AJEntityOASubmissions setOASubmissionsModel() {
    AJEntityOASubmissions oaSubmissionsModel = new AJEntityOASubmissions();
    oaSubmissionsModel.set(AJEntityOASubmissions.CLASS_ID, UUID.fromString(classId));
    oaSubmissionsModel.set(AJEntityOASubmissions.OA_ID, UUID.fromString(oaId));
    oaSubmissionsModel.set(AJEntityOASubmissions.STUDENT_ID, UUID.fromString(studentId));
    oaSubmissionsModel.set(AJEntityOASubmissions.CONTENT_SOURCE, contentSource);
    if (contentSource.equalsIgnoreCase(EventConstants.DCA)) {
      oaSubmissionsModel.set(AJEntityOASubmissions.OA_DCA_ID, oaDcaId);
    } else if (contentSource.equalsIgnoreCase(EventConstants.COURSEMAP)){
      oaSubmissionsModel.set(EventConstants.COURSE_ID, UUID.fromString(courseId));
      oaSubmissionsModel.set(EventConstants.UNIT_ID, UUID.fromString(unitId));
      oaSubmissionsModel.set(EventConstants.LESSON_ID, UUID.fromString(lessonId));
    }
    return oaSubmissionsModel;
  }

  private boolean insertOrUpdateTS() {
    AJEntityOASelfGrading oaSelfGrading = new AJEntityOASelfGrading();
    AJEntityOASelfGrading duplicateRow = null;
    if(contentSource.equalsIgnoreCase(EventConstants.DCA)) {
    duplicateRow = AJEntityOASelfGrading.findFirst(
        "student_id = ? AND oa_id = ? AND oa_dca_id = ? AND class_id = ? AND content_source = ?",
        UUID.fromString(studentId), UUID.fromString(oaId), oaDcaId, UUID.fromString(classId), contentSource);
    } else if (contentSource.equalsIgnoreCase(EventConstants.COURSEMAP)) {
      duplicateRow = AJEntityOASelfGrading.findFirst(
          "student_id = ? AND oa_id = ? AND oa_dca_id is null AND class_id = ? "
          + "AND course_id = ? AND unit_id = ? AND lesson_id = ? AND content_source = ?",
          UUID.fromString(studentId), UUID.fromString(oaId), UUID.fromString(classId), UUID.fromString(courseId),
          UUID.fromString(unitId), UUID.fromString(lessonId), contentSource);
    }
    if (duplicateRow == null && oaSelfGrading.isValid()) {
      oaSelfGrading.set(AJEntityOASelfGrading.CLASS_ID, UUID.fromString(classId));
      oaSelfGrading.set(AJEntityOASelfGrading.OA_ID, UUID.fromString(oaId));
      oaSelfGrading.set(AJEntityOASelfGrading.STUDENT_ID, UUID.fromString(studentId));
      oaSelfGrading.set(AJEntityOASelfGrading.TIMESPENT, timeSpent);
      oaSelfGrading.set(AJEntityOASelfGrading.CONTENT_SOURCE, contentSource);
      if (contentSource.equalsIgnoreCase(EventConstants.DCA)) {
        oaSelfGrading.set(AJEntityOASelfGrading.OA_DCA_ID, oaDcaId);
      } else if (contentSource.equalsIgnoreCase(EventConstants.COURSEMAP)){
        oaSelfGrading.set(EventConstants.COURSE_ID, UUID.fromString(courseId));
        oaSelfGrading.set(EventConstants.UNIT_ID, UUID.fromString(unitId));
        oaSelfGrading.set(EventConstants.LESSON_ID, UUID.fromString(lessonId));
      }
      boolean result = oaSelfGrading.insert();
      if (!result) {
        LOGGER.error("Timespent cannot be inserted for student {} & OA {} ", studentId, oaId);
        if (oaSelfGrading.hasErrors()) {
          Map<String, String> map = oaSelfGrading.errors();
          JsonObject errors = new JsonObject();
          map.forEach(errors::put);
        }
        return false;
      } else {
        LOGGER.info("Timespent inserted for student {} & OA {} ", studentId, oaId);
        return true;
      }
    } else if (duplicateRow != null && oaSelfGrading.isValid()) {
      long id = Long.valueOf(duplicateRow.get("id").toString());
      int res = Base.exec(AJEntityOASelfGrading.UPDATE_TIMESPENT_FOR_THIS_STUDENT,
          timeSpent, id);
      if (res > 0) {
        LOGGER.info("Timespent updated for student {} & OA {} ", studentId, oaId);
        return true;
      } else {
        LOGGER.error("Timespent cannot be updated for student {} & OA {} ", studentId, oaId);
        return false;
      }
    } else { // catchAll
      return false;
    }
  }

  private boolean insertRecord(AJEntityOASubmissions oaSubmissions) {
    if (oaSubmissions.isValid()) {
      boolean result = oaSubmissions.insert();
      if (!result) {
        LOGGER.error("Submission data cannot be stored for student {} & OA {} ", studentId, oaId);
        if (oaSubmissions.hasErrors()) {
          Map<String, String> map = oaSubmissions.errors();
          JsonObject errors = new JsonObject();
          map.forEach(errors::put);
        }
        return false;
      } else {
        LOGGER.info("Submission data stored for student {} & OA {} ", studentId, oaId);
        return true;
      }
    } else { // catchAll
      return false;
    }
  }
  
  private boolean insertOrUpdateSubmissionText(AJEntityOASubmissions oaSubmissions) {
    AJEntityOASubmissions duplicateRow = null;    
    if (contentSource.equalsIgnoreCase(EventConstants.DCA)) {
      duplicateRow = AJEntityOASubmissions.findFirst(
          "student_id = ?::uuid AND oa_id = ?::uuid AND oa_dca_id = ? AND class_id = ?::uuid AND task_id = ? "
              + " AND submission_info IS NOT NULL and submission_type = 'free-form-text' and content_source = ? "
              + "order by updated_at desc",
          studentId, oaId, oaDcaId, classId, oaSubmissions.get(AJEntityOASubmissions.TASK_ID),
          contentSource);
    } else if (contentSource.equalsIgnoreCase(EventConstants.COURSEMAP)) {
      duplicateRow = AJEntityOASubmissions.findFirst(
          "student_id = ?::uuid AND oa_id = ?::uuid AND oa_dca_id IS NULL AND class_id = ?::uuid AND task_id = ? "
              + " AND submission_info IS NOT NULL and submission_type = 'free-form-text' "
              + "AND course_id = ?::uuid AND unit_id = ?::uuid AND lesson_id = ?::uuid and content_source = ? order by updated_at desc",
          studentId, oaId, classId, oaSubmissions.get(AJEntityOASubmissions.TASK_ID), courseId,
          unitId, lessonId, contentSource);
    }
    if (duplicateRow == null && oaSubmissions.isValid()) {
      boolean result = oaSubmissions.insert();
      if (!result) {
        LOGGER.error("Submission text cannot be stored for student {} & OA {} ", studentId, oaId);
        if (oaSubmissions.hasErrors()) {
          Map<String, String> map = oaSubmissions.errors();
          JsonObject errors = new JsonObject();
          map.forEach(errors::put);
        }
        return false;
      } else {
        LOGGER.info("Submission text stored for student {} & OA {} ", studentId, oaId);
        return true;
      }
    } else if (duplicateRow != null && oaSubmissions.isValid()) {
      long id = Long.valueOf(duplicateRow.get("id").toString());
      int res = Base.exec(AJEntityOASubmissions.UPDATE_SUBMISSION_TEXT,
          oaSubmissions.get(AJEntityOASubmissions.SUBMISSION_INFO), new Timestamp(System.currentTimeMillis()), id);
      if (res > 0) {
        LOGGER.info("Submission Text updated for student {} & OA {} ", studentId, oaId);
        return true;
      } else {
        LOGGER.error("Submission Text cannot be updated for student {} & OA {} ", studentId, oaId);
        return false;
      }
    } else { // catchAll
      return false;
    }
  }

  @Override
  public boolean handlerReadOnly() {
    return false;
  }

}
