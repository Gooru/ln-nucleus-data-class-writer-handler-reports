package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.oa;

import java.sql.Timestamp;
import java.util.Map;
import java.util.UUID;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.gooru.nucleus.handlers.insights.events.processors.oa.OAContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOASubmissions;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.EntityBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.util.StringUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


/**
 * @author mukul@gooru
 */
public class OASubmissionsHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(OADCAEventHandler.class);
  private final OAContext context;
  public static final String COLLECTION_TYPE = "collection_type";
  private String classId;
  private String oaId;
  private Long oaDcaId;
  private String studentId;
  private JsonObject req;
  private AJEntityOASubmissions oaSubmissions;

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

    if (context.request() != null || !context.request().isEmpty()) {
      if (StringUtil.isNullOrEmpty(classId) || StringUtil.isNullOrEmpty(oaId)
          || StringUtil.isNullOrEmpty(studentId) || (oaDcaId == null)) {
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

    if (req.getJsonArray(MessageConstants.SUBMISSIONS) != null
        && !req.getJsonArray(MessageConstants.SUBMISSIONS).isEmpty()) {
      JsonArray submissions = req.getJsonArray(MessageConstants.SUBMISSIONS);
      for (Object sub : submissions) {
        AJEntityOASubmissions oaSubmissions = setOASubmissionsModel();        
        JsonObject submission = (JsonObject) sub;
        oaSubmissions.set(AJEntityOASubmissions.TASK_ID, Long.valueOf(submission.getString(AJEntityOASubmissions.TASK_ID)));
        oaSubmissions.set(AJEntityOASubmissions.SUBMISSION_INFO, submission.getString(AJEntityOASubmissions.SUBMISSION_INFO));
        oaSubmissions.set(AJEntityOASubmissions.SUBMISSION_TYPE, submission.getString(AJEntityOASubmissions.SUBMISSION_TYPE));
        oaSubmissions.set(AJEntityOASubmissions.SUBMISSION_SUBTYPE, submission.getString(AJEntityOASubmissions.SUBMISSION_SUBTYPE));
        
        if (!insertRecord(oaSubmissions)) {
          return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
              ExecutionStatus.FAILED);
        }

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
    oaSubmissionsModel.set(AJEntityOASubmissions.OA_DCA_ID, oaDcaId);
    oaSubmissionsModel.set(AJEntityOASubmissions.STUDENT_ID, UUID.fromString(studentId));
    
    return oaSubmissionsModel;
  }


  private boolean insertRecord(AJEntityOASubmissions oaSubmissions) {
    if (oaSubmissions.isValid()) {
      boolean result = oaSubmissions.insert();
      if (!result) {
        LOGGER.error("Submission data cannot be stored for student {} & OA {} ",
            studentId, oaId);
        if (oaSubmissions.hasErrors()) {
          Map<String, String> map = oaSubmissions.errors();
          JsonObject errors = new JsonObject();
          map.forEach(errors::put);
        }
        return false;
      } else {
        LOGGER.info("Submission data stored for student {} & OA {} ", studentId,
            oaId);
        return true;
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
