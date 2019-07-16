package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.oa;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.processors.oa.OAContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOACompletionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOASelfGrading;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOASubmissions;
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
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author mukul@gooru
 */
public class OADCAEventHandler implements DBHandler {
  
  private static final Logger LOGGER =
      LoggerFactory.getLogger(OADCAEventHandler.class);
  private static final String USERS = "users";
  private static final String UTC = "Etc/UTC";
  private final OAContext context;
  private String classId;
  private String oaId;
  private Long oaDcaId;
  private String contentSource;
  private String localeDate;
  private long ts;
  private long pathId = 0L;
  private String pathType = null;
  private JsonArray users;
  List<String> selfGradedList = new ArrayList<>();
  List<String> oaStudentsList = new ArrayList<>();

  public OADCAEventHandler(OAContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {  

    classId = context.request().getString(AJEntityDailyClassActivity.CLASS_GOORU_OID);
    oaId = context.request().getString(AJEntityOASubmissions.OA_ID);
    oaDcaId = context.request().getLong(AJEntityOASubmissions.OA_DCA_ID);
    users = context.request().getJsonArray(USERS);
    contentSource = context.request().getString(AJEntityOACompletionStatus.CONTENT_SOURCE);
    pathId = context.request().getLong(EventConstants._PATH_ID, 0L);
    pathType = context.request().getString(EventConstants._PATH_TYPE);
    
    if (!ValidationUtils.isValidUUID(classId) || !ValidationUtils.isValidUUID(oaId)
        || oaDcaId == null || users == null || (StringUtil.isNullOrEmptyAfterTrim(contentSource)
            || (contentSource != null && !contentSource.equalsIgnoreCase(EventConstants.DCA)))) {
      LOGGER.warn("Invalid Json Payload");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
          ExecutionStatus.FAILED);
    }
    
    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    //API Set to INTERNAL - bypass validation
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    oaStudentsList = IntStream.range(0,users.size()).mapToObj(i-> users.getString(i)).collect(Collectors.toList());
    ts = System.currentTimeMillis();

    //If studentList from the request is empty DO NOTHING
    if (!oaStudentsList.isEmpty()) {
      List<Map> oaPerfList = Base.findAll(AJEntityOASelfGrading.GET_OA_PERFORMANCE, 
          UUID.fromString(oaId), oaDcaId, listToPostgresArrayString(oaStudentsList), UUID.fromString(classId), contentSource);
      if (!oaPerfList.isEmpty()) {
        for (Map m : oaPerfList ) {
          AJEntityDailyClassActivity dca = setDCAModel();
          String studentId = m.get(AJEntityOASelfGrading.STUDENT_ID).toString();
          selfGradedList.add(studentId);
          dca.set(AJEntityDailyClassActivity.GOORUUID, studentId);       
          dca.set(AJEntityDailyClassActivity.TIMESPENT, m.get(AJEntityOASelfGrading.TIMESPENT) != null
              ? Long.valueOf(m.get(AJEntityOASelfGrading.TIMESPENT).toString())
                  : 0L);
          dca.set(AJEntityDailyClassActivity.SCORE, m.get(AJEntityOASelfGrading.STUDENT_SCORE) != null
              ? Double.valueOf(m.get(AJEntityOASelfGrading.STUDENT_SCORE).toString())
                  : null);
          dca.set(AJEntityDailyClassActivity.MAX_SCORE, m.get(AJEntityOASelfGrading.MAX_SCORE) != null
              ? Double.valueOf(m.get(AJEntityOASelfGrading.MAX_SCORE).toString())
                  : null);
          //If a record already exists for this student, then we UPDATE the Perf & TS else we Insert
          if (!insertOrUpdateRecord(studentId, dca)) {
            return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
                ExecutionStatus.FAILED);
          }
        }
      }
      
      ExecutionResult<MessageResponse> executionStatus = storeOACompletionStatus();
      if (executionStatus.hasFailed()) {
        return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
            ExecutionStatus.FAILED);
      }
       
      //Students have not graded their tasks, and may or may not have submissions.
      getNotSelfGradedList();
      if (oaStudentsList != null && !oaStudentsList.isEmpty()) {
        for (String studentId : oaStudentsList) {
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
    dcaModel.set(AJEntityDailyClassActivity.CONTENT_SOURCE, contentSource);
    dcaModel.set(AJEntityDailyClassActivity.TIME_ZONE, UTC);
    dcaModel.set(AJEntityDailyClassActivity.RESOURCE_ATTEMPT_STATUS, EventConstants.ATTEMPTED);
    setDateInTimeZone();
    if (localeDate != null) {
      dcaModel.setDateinTZ(localeDate);      
    }    
    dcaModel.set(AJEntityDailyClassActivity.PATH_ID, pathId);
    dcaModel.set(AJEntityDailyClassActivity.PATH_TYPE, pathType);    
    return dcaModel;
  }
  
  private boolean insertOrUpdateRecord(String studentId, AJEntityDailyClassActivity dca) {
    AJEntityDailyClassActivity duplicateRow = null;
    duplicateRow = AJEntityDailyClassActivity.findFirst("actor_id = ? AND collection_id = ? "
        + "AND dca_content_id = ? AND collection_type = 'offline-activity' "
        + "AND event_name = 'collection.play' "
        + "AND event_type = 'stop' AND content_source = ?", studentId, oaId, oaDcaId, contentSource);
    
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

  private void getNotSelfGradedList() {
    if (selfGradedList != null && oaStudentsList != null && !oaStudentsList.isEmpty()) {
      oaStudentsList.removeAll(selfGradedList);
    }    
  }
  
  private void setDateInTimeZone() {
    localeDate = BaseUtil.UTCDate(ts);
  }
  
  
  private String listToPostgresArrayString(List<String> input) {
    int approxSize = ((input.size() + 1) * 36);
    Iterator<String> it = input.iterator();
    if (!it.hasNext()) {
      return "{}";
    }
    StringBuilder sb = new StringBuilder(approxSize);
    sb.append('{');
    for (;;) {
      String s = it.next();
      sb.append('"').append(s).append('"');
      if (!it.hasNext()) {
        return sb.append('}').toString();
      }
      sb.append(',');
    }
  }
  
  private ExecutionResult<MessageResponse> storeOACompletionStatus() {
    if (oaStudentsList != null ) {
      for (String studentId: oaStudentsList) {
        AJEntityOACompletionStatus oacs = new AJEntityOACompletionStatus();
        oacs.set(AJEntityOACompletionStatus.CLASS_ID, UUID.fromString(classId));
        oacs.set(AJEntityOACompletionStatus.OA_ID, UUID.fromString(oaId));
        oacs.set(AJEntityOACompletionStatus.OA_DCA_ID, oaDcaId);
        oacs.set(AJEntityOACompletionStatus.STUDENT_ID, UUID.fromString(studentId));
        oacs.set(AJEntityOACompletionStatus.COLLECTION_TYPE, EventConstants.OFFLINE_ACTIVITY);
        oacs.set(AJEntityOACompletionStatus.CONTENT_SOURCE, contentSource);
        oacs.set(AJEntityOACompletionStatus.IS_MARKED_BY_TEACHER, true);
        oacs.set(AJEntityOACompletionStatus.PATH_ID, pathId);
        oacs.set(AJEntityOACompletionStatus.PATH_TYPE, pathType);
        if (!insertOrUpdateCompletionRecord(studentId, oacs)) {
         return new ExecutionResult<>(null, ExecutionStatus.FAILED);
        }
      }
    }
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }
  
  private boolean insertOrUpdateCompletionRecord(String studentId, AJEntityOACompletionStatus oacs) {
    AJEntityOACompletionStatus duplicateRow = AJEntityOACompletionStatus.findFirst(AJEntityOACompletionStatus.GET_DCA_OA_COMPLETION_STATUS, studentId, oaId, oaDcaId, classId, contentSource);
    
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
      res = Base.exec(AJEntityOACompletionStatus.UPDATE_OA_COMPLETION_STATUS_BY_TEACHER, true,
          new Timestamp(System.currentTimeMillis()), id);
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

}
