package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.oa;

import java.sql.Timestamp;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.processors.oa.OAContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOACompletionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOASelfGrading;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.utils.BaseUtil;
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
 * @author renuka
 */
public class OACompletionEventHandler implements DBHandler {
  
  private static final Logger LOGGER =
      LoggerFactory.getLogger(OACompletionEventHandler.class);
  private static final String UTC = "Etc/UTC";
  private Pattern VALID_MARKED_BY_TYPE = Pattern.compile("student|teacher");
  private final OAContext context;
  private String classId;
  private String oaId;
  private Long oaDcaId;
  private String localeDate;
  private long ts;
  private long pathId = 0L;
  private String pathType = null;
  private String studentId;
  private String markedBy;
  private String contentSource;

  public OACompletionEventHandler(OAContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {  
    classId = context.request().getString(AJEntityOACompletionStatus.CLASS_ID);
    oaId = context.request().getString(AJEntityOACompletionStatus.OA_ID);
    oaDcaId = context.request().getLong(AJEntityOACompletionStatus.OA_DCA_ID);
    studentId = context.request().getString(AJEntityOACompletionStatus.STUDENT_ID);
    markedBy = context.request().getString(AJEntityOACompletionStatus.MARKED_BY);
    contentSource = context.request().getString(AJEntityOACompletionStatus.CONTENT_SOURCE);

    if (!ValidationUtils.isValidUUID(classId) || !ValidationUtils.isValidUUID(oaId)
        || !ValidationUtils.isValidUUID(studentId) || oaDcaId == null
        || StringUtil.isNullOrEmptyAfterTrim(markedBy)
        || (markedBy != null && !VALID_MARKED_BY_TYPE.matcher(markedBy).matches())
        || StringUtil.isNullOrEmptyAfterTrim(contentSource)) {
      LOGGER.warn("Invalid Json Payload");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
          ExecutionStatus.FAILED);
    }
    
    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    if (StringUtil.isNullOrEmpty(context.request().getString("userIdFromSession"))
        || (context.request().getString("userIdFromSession") != null
            && !context.request().getString("userIdFromSession").equals(studentId))) {
      return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("Auth Failure"),
          ExecutionStatus.FAILED);
    }
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    ts = System.currentTimeMillis();
    AJEntityOACompletionStatus oacs = setOACompletionStatusModel();
    // If a record already exists for this student, then we UPDATE the Perf & TS else we Insert
    if (!insertOrUpdateCompletionRecord(studentId, oacs)) {
      return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
          ExecutionStatus.FAILED);
    }

    AJEntityOASelfGrading oaPerf =
        AJEntityOASelfGrading.findFirst(AJEntityOASelfGrading.GET_OA_PERFORMANCE_FOR_STUDENT,
            UUID.fromString(oaId), oaDcaId, studentId, UUID.fromString(classId));
    if (oaPerf != null) {
      AJEntityDailyClassActivity dca = setDCAModel();
      dca.set(AJEntityDailyClassActivity.GOORUUID, studentId);
      dca.set(AJEntityDailyClassActivity.TIMESPENT,
          oaPerf.get(AJEntityOASelfGrading.TIMESPENT) != null
              ? Long.valueOf(oaPerf.get(AJEntityOASelfGrading.TIMESPENT).toString())
              : 0L);
      dca.set(AJEntityDailyClassActivity.SCORE,
          oaPerf.get(AJEntityOASelfGrading.STUDENT_SCORE) != null
              ? Double.valueOf(oaPerf.get(AJEntityOASelfGrading.STUDENT_SCORE).toString())
              : null);
      dca.set(AJEntityDailyClassActivity.MAX_SCORE,
          oaPerf.get(AJEntityOASelfGrading.MAX_SCORE) != null
              ? Double.valueOf(oaPerf.get(AJEntityOASelfGrading.MAX_SCORE).toString())
              : null);
      // If a record already exists for this student, then we UPDATE the Perf & TS else we Insert
      if (!insertOrUpdateRecord(studentId, dca)) {
        return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
            ExecutionStatus.FAILED);
      }

    } else {
      AJEntityDailyClassActivity notSelfGradedDca = setDCAModel();
      notSelfGradedDca.set(AJEntityDailyClassActivity.GOORUUID, studentId);
      notSelfGradedDca.set(AJEntityDailyClassActivity.TIMESPENT, 0L);
      notSelfGradedDca.set(AJEntityDailyClassActivity.SCORE, null);
      notSelfGradedDca.set(AJEntityDailyClassActivity.MAX_SCORE, null);

      if (!insertOrUpdateRecord(studentId, notSelfGradedDca)) {
        return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
            ExecutionStatus.FAILED);
      }
    }

    return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(),
        ExecutionStatus.SUCCESSFUL);

  }

  @Override
  public boolean handlerReadOnly() {
    return false;
  }

  private AJEntityDailyClassActivity setDCAModel() {
    AJEntityDailyClassActivity dcaModel = new AJEntityDailyClassActivity();
    dcaModel.set(AJEntityDailyClassActivity.IS_GRADED, false);
    dcaModel.set(AJEntityDailyClassActivity.CLASS_GOORU_OID, classId);
    dcaModel.set(AJEntityDailyClassActivity.COLLECTION_OID, oaId);
    dcaModel.set(AJEntityDailyClassActivity.DCA_CONTENT_ID, oaDcaId);
    dcaModel.set(AJEntityDailyClassActivity.COLLECTION_TYPE, EventConstants.OFFLINE_ACTIVITY);
    dcaModel.set(AJEntityDailyClassActivity.RESOURCE_TYPE, EventConstants.NA);
    dcaModel.set(AJEntityDailyClassActivity.QUESTION_TYPE, EventConstants.NA);
    dcaModel.set(AJEntityDailyClassActivity.EVENTNAME, EventConstants.COLLECTION_PLAY);
    dcaModel.set(AJEntityDailyClassActivity.EVENTTYPE, EventConstants.STOP);
    dcaModel.set(AJEntityDailyClassActivity.CREATE_TIMESTAMP, new Timestamp(ts));
    dcaModel.set(AJEntityDailyClassActivity.UPDATE_TIMESTAMP, new Timestamp(ts));
    dcaModel.set(AJEntityDailyClassActivity.GRADING_TYPE, EventConstants.TEACHER); 
    dcaModel.set(AJEntityDailyClassActivity.SESSION_ID, UUID.randomUUID().toString());
    dcaModel.set(AJEntityDailyClassActivity.CONTENT_SOURCE, EventConstants.DCA);
    dcaModel.set(AJEntityDailyClassActivity.TIME_ZONE, UTC);
    dcaModel.set(AJEntityDailyClassActivity.RESOURCE_ATTEMPT_STATUS, EventConstants.ATTEMPTED);
    setDateInTimeZone();
    if (localeDate != null) {
      dcaModel.setDateinTZ(localeDate);      
    }    
    //Since Offline-Activities are not designed to be Suggestions, path_id & path_type SHOULD be 0L & null resp.
    pathId = context.request().containsKey(AJEntityDailyClassActivity.PATH_ID) ? context.request().getLong(AJEntityDailyClassActivity.PATH_ID) : 0L;
    pathType = context.request().containsKey(AJEntityDailyClassActivity.PATH_TYPE) ? context.request().getString(AJEntityDailyClassActivity.PATH_TYPE) : null;
    dcaModel.set(AJEntityDailyClassActivity.PATH_ID, pathId);
    dcaModel.set(AJEntityDailyClassActivity.PATH_TYPE, pathType);    
    return dcaModel;
  }
  
  private boolean insertOrUpdateRecord(String studentId, AJEntityDailyClassActivity dca) {
    AJEntityDailyClassActivity duplicateRow = null;
    duplicateRow = AJEntityDailyClassActivity.findFirst("actor_id = ? AND collection_id = ? "
        + "AND dca_content_id = ? AND collection_type = 'offline-activity' "
        + "AND event_name = 'collection.play' "
        + "AND event_type = 'stop'", studentId, oaId, oaDcaId);
    
    if (duplicateRow == null && dca.isValid()) {
      boolean result = dca.insert();
      if (!result) {
        LOGGER.error("Offline Activity Record cannot be inserted into dca for student {} & OA {} ", studentId, oaId);
        if (dca.hasErrors()) {
          Map<String, String> map = dca.errors();
          JsonObject errors = new JsonObject();
          map.forEach(errors::put);          
        }
        return false;
      } else {
        LOGGER.info("Offline Activity Record Inserted into dca for student {} & OA {} ", studentId, oaId);
        return true;
      }
    } else if (duplicateRow != null && dca.isValid()){
      long id = Long.valueOf(duplicateRow.get("id").toString());
      int res = Base.exec(AJEntityDailyClassActivity.UPDATE_OA_RECORD_FOR_THIS_STUDENT,
          dca.get(AJEntityDailyClassActivity.TIMESPENT),
          dca.get(AJEntityDailyClassActivity.SCORE),
          dca.get(AJEntityDailyClassActivity.MAX_SCORE),
          new Timestamp(System.currentTimeMillis()), id);
      if (res > 0) {
        LOGGER.info("Offline Activity Record updated into dca for student {} & OA {} ", studentId, oaId);
        return true;
      } else {
        LOGGER.error("Offline Activity Record cannot be updated into dca for student {} & OA {} ", studentId, oaId);
        return false;
      }  
    } else { //catchAll
      return false;
    }
  }
  
  private AJEntityOACompletionStatus setOACompletionStatusModel() {
    AJEntityOACompletionStatus oacs = new AJEntityOACompletionStatus();
    oacs.set(AJEntityOACompletionStatus.CLASS_ID, UUID.fromString(classId));
    oacs.set(AJEntityOACompletionStatus.OA_ID, UUID.fromString(oaId));
    oacs.set(AJEntityOACompletionStatus.OA_DCA_ID, oaDcaId);
    oacs.set(AJEntityOACompletionStatus.STUDENT_ID, UUID.fromString(studentId));
    oacs.set(AJEntityOACompletionStatus.COLLECTION_TYPE, EventConstants.OFFLINE_ACTIVITY);
    oacs.set(AJEntityOACompletionStatus.CONTENT_SOURCE, EventConstants.DCA);
    if (markedBy.equalsIgnoreCase(EventConstants.STUDENT)) {
      oacs.set(AJEntityOACompletionStatus.IS_MARKED_BY_STUDENT, true);
    } else {
      oacs.set(AJEntityOACompletionStatus.IS_MARKED_BY_TEACHER, true);
    }
    return oacs;
  }
  
  private boolean insertOrUpdateCompletionRecord(String studentId, AJEntityOACompletionStatus oacs) {
    AJEntityOACompletionStatus duplicateRow =
        AJEntityOACompletionStatus.findFirst(AJEntityOACompletionStatus.GET_OA_COMPLETION_STATUS,
            studentId, oaId, oaDcaId, classId, contentSource);
    
    if (duplicateRow == null && oacs.isValid()) {
      boolean result = oacs.insert();
      if (!result) {
        LOGGER.error("Offline Activity Completion status cannot be inserted into oacs table {} & OA {} ", studentId, oaId);
        if (oacs.hasErrors()) {
          Map<String, String> map = oacs.errors();
          JsonObject errors = new JsonObject();
          map.forEach(errors::put);          
        }
        return false;
      } else {
        LOGGER.info("Offline Activity Completion status Inserted into oacs table {} & OA {} ", studentId, oaId);
        return true;
      }
    } else if (duplicateRow != null && oacs.isValid()){
      long id = Long.valueOf(duplicateRow.get("id").toString());
      int res = 0;
      if (markedBy.equalsIgnoreCase(EventConstants.STUDENT)) {
        res = Base.exec(AJEntityOACompletionStatus.UPDATE_OA_COMPLETION_STATUS_BY_STUDENT, true,
            new Timestamp(System.currentTimeMillis()), id, contentSource);
      } else {
        res = Base.exec(AJEntityOACompletionStatus.UPDATE_OA_COMPLETION_STATUS_BY_TEACHER, true,
            new Timestamp(System.currentTimeMillis()), id, contentSource);
      }
      if (res > 0) {
        LOGGER.info("Offline Activity Completion status updated into oacs table {} & OA {} ", studentId, oaId);
        return true;
      } else {
        LOGGER.error("Offline Activity Completion status cannot be updated into oacs table {} & OA {} ", studentId, oaId);
        return false;
      }  
    } else { //catchAll
      return false;
    }
  }
  
  private void setDateInTimeZone() {
    localeDate = BaseUtil.UTCDate(ts);
  }

}
