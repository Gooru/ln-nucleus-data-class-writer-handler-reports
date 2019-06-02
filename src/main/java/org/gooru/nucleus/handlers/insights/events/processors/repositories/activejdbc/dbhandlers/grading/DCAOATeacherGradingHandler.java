package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.grading;

import java.sql.Timestamp;
import java.util.Map;
import org.gooru.nucleus.handlers.insights.events.processors.grading.GradingContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOASelfGrading;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityRubricGrading;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.EntityBuilder;
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
public class DCAOATeacherGradingHandler implements DBHandler {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(DCAOATeacherGradingHandler.class);
  private AJEntityRubricGrading rubricGrading;
  private final GradingContext context;
  private JsonObject req;
  private String classId;
  private String studentId;
  private Long dcaContentId;
  private String collectionId;
  private Double score;
  private Double max_score;
  
  public DCAOATeacherGradingHandler(GradingContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    req = context.request();
    classId = context.request().getString(AJEntityRubricGrading.CLASS_ID);
    dcaContentId = context.request().getLong(AJEntityRubricGrading.DCA_CONTENT_ID);
    studentId = context.request().getString(AJEntityRubricGrading.STUDENT_ID);
    collectionId = context.request().getString(AJEntityRubricGrading.COLLECTION_ID);

    if (context.request() != null || !context.request().isEmpty()) {
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
    max_score = req.getDouble(AJEntityRubricGrading.MAX_SCORE);
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
        return true;
      }
    } else if (duplicateRow != null && rubricGrading.isValid()){
      long id = Long.valueOf(duplicateRow.get("id").toString());
      int res = Base.exec(AJEntityRubricGrading.UPDATE_COLLECTION_GRADES,
          rubricGrading.get(AJEntityOASelfGrading.STUDENT_SCORE),
          rubricGrading.get(AJEntityOASelfGrading.MAX_SCORE),
          rubricGrading.get(AJEntityOASelfGrading.CATEGORY_GRADE),
          rubricGrading.get(AJEntityOASelfGrading.OVERALL_COMMENT),
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
      result = Base.exec(AJEntityDailyClassActivity.UPDATE_OA_SCORE, score, max_score, true, studentId, collectionId, dcaContentId);
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
  
  
  @Override
  public boolean handlerReadOnly() {
    return false;
  }

}

