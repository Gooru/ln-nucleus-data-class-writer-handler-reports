package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.grading;

import java.sql.Timestamp;
import java.util.Map;
import java.util.UUID;
import org.gooru.nucleus.handlers.insights.events.processors.grading.GradingContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOACompletionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOASelfGrading;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOASubmissions;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.EntityBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.validators.ValidationUtils;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.util.StringUtil;
import io.vertx.core.json.JsonObject;

/**
 * @author mukul@gooru
 */
public class DCAOASelfGradingHandler implements DBHandler {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(DCAOASelfGradingHandler.class);
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

  public DCAOASelfGradingHandler(GradingContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    req = context.request();
    classId = context.request().getString(AJEntityOASelfGrading.CLASS_ID);
    oaId = context.request().getString(COLLECTION_ID);
    oaDcaId = context.request().getLong(DCA_CONTENT_ID);
    studentId = context.request().getString(AJEntityOASelfGrading.STUDENT_ID);
    contentSource = context.request().getString(AJEntityOASubmissions.CONTENT_SOURCE);

    if (context.request() != null || !context.request().isEmpty()) {
      if (!ValidationUtils.isValidUUID(classId) || !ValidationUtils.isValidUUID(oaId)
          || !ValidationUtils.isValidUUID(studentId) || (oaDcaId == null)
          || StringUtil.isNullOrEmptyAfterTrim(contentSource)) {
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

  @SuppressWarnings("rawtypes")
  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    // Student validation
    if (context.request().getString("userIdFromSession") != null) {
      if (!context.request().getString("userIdFromSession")
          .equals(studentId)) {
        return new
            ExecutionResult<>(MessageResponseFactory.createForbiddenResponse
            ("Auth Failure"), ExecutionStatus.FAILED);
      }
    } else if (StringUtil.isNullOrEmpty(context.request().getString("userIdFromSession"))) {
      return new
          ExecutionResult<>(MessageResponseFactory.createForbiddenResponse
          ("Auth Failure"), ExecutionStatus.FAILED);
    }
    AJEntityOACompletionStatus isOAMarkedComplete =
        AJEntityOACompletionStatus.findFirst(AJEntityOACompletionStatus.GET_OA_MARKED_AS_COMPLETED,
            studentId, oaId, oaDcaId, classId, contentSource);
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

  private void prune() {
    req.remove(COLLECTION_TYPE);
    req.remove(COLLECTION_ID);
    req.remove(DCA_CONTENT_ID);
    req.remove("userIdFromSession");
  }

  private void mapOAAttributes() {    
   oaSelfGrading.set(AJEntityOASelfGrading.OA_ID, UUID.fromString(oaId));
   oaSelfGrading.set(AJEntityOASelfGrading.OA_DCA_ID, oaDcaId);
  }
  private boolean insertOrUpdateRecord(AJEntityOASelfGrading oaSelfGrading) {
    AJEntityOASelfGrading duplicateRow = null;
    duplicateRow = AJEntityOASelfGrading.findFirst("student_id = ? AND oa_id = ? AND oa_dca_id = ? AND class_id = ?", 
        UUID.fromString(studentId), UUID.fromString(oaId), oaDcaId, UUID.fromString(classId));
    
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

