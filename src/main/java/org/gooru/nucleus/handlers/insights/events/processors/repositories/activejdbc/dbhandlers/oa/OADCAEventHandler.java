package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.oa;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.processors.oa.OAContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOASelfGrading;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOASubmissions;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.utils.BaseUtil;
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
public class OADCAEventHandler implements DBHandler {
  
  private static final Logger LOGGER =
      LoggerFactory.getLogger(OADCAEventHandler.class);
  private static final String UTC = "Etc/UTC";
  private final OAContext context;
  private String classId;
  private String oaId;
  private Long oaDcaId;
//  private String timeZone;
  private String localeDate;
  private long ts;
  private long pathId = 0L;
  private String pathType = null;
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
//    timeZone = context.request().getString(AJEntityDailyClassActivity.TIME_ZONE);    
        
    if (StringUtil.isNullOrEmpty(classId) || StringUtil.isNullOrEmpty(oaId)) {
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
    ts = System.currentTimeMillis();
    List oaStudents = 
        Base.firstColumn(AJEntityOASubmissions.GET_STUDENTS_FOR_OA, UUID.fromString(classId), UUID.fromString(oaId), oaDcaId);
    //TODO: Handle case when User has been assigned an OA, but he has not turned in any evidences
    for (Object s : oaStudents) { 
      oaStudentsList.add(s.toString());     
    }
    
    List<Map> oaPerfList = Base.findAll(AJEntityOASelfGrading.GET_OA_PERFORMANCE, 
        UUID.fromString(oaId), oaDcaId, listToPostgresArrayString(oaStudentsList), UUID.fromString(classId));
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
    
    //Student has submitted evidences, but he has not graded his tasks
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
    dcaModel.set(AJEntityDailyClassActivity.TENANT_ID, context.request().getString(AJEntityDailyClassActivity.TENANT_ID));
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

}
