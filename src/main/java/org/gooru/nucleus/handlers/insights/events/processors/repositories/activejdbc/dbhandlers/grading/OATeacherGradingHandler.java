package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.grading;

import static org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.validators.ValidationUtils.validateScoreAndMaxScore;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.GEPConstants;
import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.gooru.nucleus.handlers.insights.events.processors.grading.GradingContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.GEPEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.RDAEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOACompletionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOASelfGrading;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityOASubmissions;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityRubricGrading;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.EntityBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.utils.BaseUtil;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.validators.ValidationUtils;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.util.StringUtil;
import io.vertx.core.json.JsonObject;


/**
 * @author mukul@gooru
 */
public class OATeacherGradingHandler implements DBHandler {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(OATeacherGradingHandler.class);
  public static final String TOPIC_GEP = "gep";
  public static final String OA_TYPE = "offline-activity";
  private static final String UTC = "Etc/UTC";
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
  private String timezone;
  private long ts;
  private String sessionId;
  
  public OATeacherGradingHandler(GradingContext context) {
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
              && ValidationUtils.isValidUUID(unitId) && ValidationUtils.isValidUUID(lessonId)))
          || (score != null && maxScore != null && !validateScoreAndMaxScore(score, maxScore))) {
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
    pathId = req.getLong(EventConstants._PATH_ID, 0L);
    pathType = req.getString(EventConstants._PATH_TYPE);
    score = req.getDouble(AJEntityRubricGrading.STUDENT_SCORE);
    maxScore = req.getDouble(AJEntityRubricGrading.MAX_SCORE);
    timezone = req.getString(AJEntityDailyClassActivity.TIME_ZONE, UTC);
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
      ExecutionResult<MessageResponse> cmExecResponse = insertBaseReportsData();
      if (cmExecResponse.hasFailed()) {
        return cmExecResponse;
      }
    }
    
    if (!insertOrUpdateCompletionRecord(studentId)) {
      return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
          ExecutionStatus.FAILED);
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
        LOGGER.error("Teacher Grades cannot be inserted for student {} & OA {} ", studentId, collectionId);
        if (rubricGrading.hasErrors()) {
          Map<String, String> map = rubricGrading.errors();
          JsonObject errors = new JsonObject();
          map.forEach(errors::put);          
        }
        return false;
      } else {
        LOGGER.info("Teacher Grades inserted for student {} & OA {} ", studentId, collectionId);
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
        LOGGER.info("Teacher Grades updated for student {} & OA {} ", studentId, collectionId);
        return true;
      } else {
        LOGGER.error("Teacher Grades cannot be updated for student {} & OA {} ", studentId, collectionId);
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
  
  private ExecutionResult<MessageResponse> insertBaseReportsData() {
    AJEntityReporting baseReports = new AJEntityReporting();
    setBaseReportsModel(baseReports);
    if (!insertOrUpdateBaseReportsRecord(studentId, baseReports)) {
      return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
          ExecutionStatus.FAILED);
    }
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }
  
  private Model setBaseReportsModel(AJEntityReporting model) {
    ts = System.currentTimeMillis();
    sessionId = UUID.randomUUID().toString();
    model.set(AJEntityReporting.GOORUUID, studentId);
    model.set(AJEntityReporting.IS_GRADED, true);
    model.set(AJEntityReporting.CLASS_GOORU_OID, classId);
    model.set(AJEntityReporting.COLLECTION_OID, collectionId);
    model.set(AJEntityReporting.COLLECTION_TYPE, EventConstants.OFFLINE_ACTIVITY);
    model.set(AJEntityReporting.RESOURCE_TYPE, EventConstants.NA);
    model.set(AJEntityReporting.QUESTION_TYPE, EventConstants.NA);
    model.set(AJEntityReporting.EVENTNAME, EventConstants.COLLECTION_PLAY);
    model.set(AJEntityReporting.EVENTTYPE, EventConstants.STOP);
    model.set(AJEntityReporting.CREATE_TIMESTAMP, new Timestamp(ts));
    model.set(AJEntityReporting.UPDATE_TIMESTAMP, new Timestamp(ts));
    model.set(AJEntityReporting.GRADING_TYPE, EventConstants.TEACHER);
    model.set(AJEntityReporting.SESSION_ID, sessionId);
    model.set(AJEntityReporting.CONTENT_SOURCE, contentSource);
    model.set(AJEntityReporting.TIME_ZONE, timezone);
    model.set(AJEntityReporting.RESOURCE_ATTEMPT_STATUS, EventConstants.ATTEMPTED);
    model.set(AJEntityReporting.COURSE_GOORU_OID, courseId);
    model.set(AJEntityReporting.UNIT_GOORU_OID, unitId);
    model.set(AJEntityReporting.LESSON_GOORU_OID, lessonId);
    model.set(AJEntityReporting.PATH_ID, pathId);
    model.set(AJEntityReporting.PATH_TYPE, pathType);
    
    String localeDate = BaseUtil.UTCToLocale(ts, timezone);
    if (localeDate != null) {
      model.setDateinTZ(localeDate);
    }
    Double scoreInPercent = null;
    if (validateScoreAndMaxScore(score, maxScore)) {
      scoreInPercent = ((score * 100) / maxScore);
      LOGGER.debug("Re-Computed total score {} ", scoreInPercent);
    }
    model.set(AJEntityReporting.SCORE, scoreInPercent);
    model.set(AJEntityReporting.MAX_SCORE, maxScore);
    
    AJEntityOASelfGrading oaPerf = AJEntityOASelfGrading.findFirst(AJEntityOASelfGrading.GET_CM_OA_PERFORMANCE_FOR_STUDENT,
        collectionId, studentId, classId, courseId, unitId, lessonId, contentSource);
    model.set(AJEntityReporting.TIMESPENT, (oaPerf != null && oaPerf.get(AJEntityOASelfGrading.TIMESPENT) != null) ? Long.valueOf(oaPerf.get(AJEntityOASelfGrading.TIMESPENT).toString()) : 0L);
    return model;
  }
  
  private boolean insertOrUpdateBaseReportsRecord(String studentId, AJEntityReporting baseReports) {
    AJEntityReporting duplicateRow = null;
    duplicateRow = AJEntityReporting.findFirst(
        "actor_id = ? AND collection_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? "
            + "AND content_source = ? AND collection_type = 'offline-activity' "
            + "AND event_name = 'collection.play' AND event_type = 'stop'",
        studentId, collectionId, courseId, unitId, lessonId, contentSource);

    if (duplicateRow == null && baseReports.isValid()) {
      boolean result = baseReports.insert();
      if (!result) {
        LOGGER.error(
            "Offline Activity Record cannot be inserted into base reports for student {} & OA {} ",
            studentId, collectionId);
        if (baseReports.hasErrors()) {
          Map<String, String> map = baseReports.errors();
          JsonObject errors = new JsonObject();
          map.forEach(errors::put);
        }
        return false;
      } else {
        LOGGER.info("Offline Activity Record Inserted into base reports for student {} & OA {} ",
            studentId, collectionId);
        return true;
      }
    } else if (duplicateRow != null && baseReports.isValid()) {
      long id = Long.valueOf(duplicateRow.get("id").toString());
      int res = Base.exec(AJEntityReporting.UPDATE_OA_RECORD_FOR_THIS_STUDENT,
          baseReports.get(AJEntityReporting.TIMESPENT), baseReports.get(AJEntityReporting.SCORE),
          baseReports.get(AJEntityReporting.MAX_SCORE), new Timestamp(ts),
          id);
      if (res > 0) {
        LOGGER.info("Offline Activity Record updated into base reports for student {} & OA {} ",
            studentId, collectionId);
        return true;
      } else {
        LOGGER.error(
            "Offline Activity Record cannot be updated into base reports for student {} & OA {} ",
            studentId, collectionId);
        return false;
      }
    } else { // catchAll
      return false;
    }
  }
  
  private AJEntityOACompletionStatus setOACompletionStatusModel() {
    AJEntityOACompletionStatus oacs = new AJEntityOACompletionStatus();
    oacs.set(AJEntityOACompletionStatus.CLASS_ID, UUID.fromString(classId));
    oacs.set(AJEntityOACompletionStatus.OA_ID, UUID.fromString(collectionId));
    oacs.set(AJEntityOACompletionStatus.STUDENT_ID, UUID.fromString(studentId));
    oacs.set(AJEntityOACompletionStatus.COLLECTION_TYPE, EventConstants.OFFLINE_ACTIVITY);
    oacs.set(AJEntityOACompletionStatus.CONTENT_SOURCE, contentSource);
    oacs.set(AJEntityOACompletionStatus.IS_TEACHER_GRADED, true);
    oacs.set(AJEntityOACompletionStatus.PATH_ID, pathId);
    oacs.set(AJEntityOACompletionStatus.PATH_TYPE, pathType);
    if (contentSource.equalsIgnoreCase(EventConstants.DCA)) {
      oacs.set(AJEntityOACompletionStatus.OA_DCA_ID, dcaContentId);
    } else if (contentSource.equalsIgnoreCase(EventConstants.COURSEMAP)) {
      oacs.set(EventConstants.COURSE_ID, UUID.fromString(courseId));
      oacs.set(EventConstants.UNIT_ID, UUID.fromString(unitId));
      oacs.set(EventConstants.LESSON_ID, UUID.fromString(lessonId));
    }
    return oacs;
  }

  private boolean insertOrUpdateCompletionRecord(String studentId) {
    AJEntityOACompletionStatus oacs = setOACompletionStatusModel();
    AJEntityOACompletionStatus duplicateRow = null;
    if (contentSource.equalsIgnoreCase(EventConstants.DCA)) {
      duplicateRow = AJEntityOACompletionStatus.findFirst(
          AJEntityOACompletionStatus.GET_DCA_OA_COMPLETION_STATUS, studentId, collectionId, dcaContentId,
          classId, contentSource);
    } else if (contentSource.equalsIgnoreCase(EventConstants.COURSEMAP)) {
      duplicateRow = AJEntityOACompletionStatus.findFirst(
          AJEntityOACompletionStatus.GET_CM_OA_COMPLETION_STATUS, studentId, collectionId, classId,
          courseId, unitId, lessonId, contentSource);
    }

    if (duplicateRow == null && oacs.isValid()) {
      boolean result = oacs.insert();
      if (!result) {
        LOGGER.error(
            "Offline Activity teacher graded flag cannot be inserted into oacs table {} & OA {} ",
            studentId, collectionId);
        if (oacs.hasErrors()) {
          Map<String, String> map = oacs.errors();
          JsonObject errors = new JsonObject();
          map.forEach(errors::put);
        }
        return false;
      } else {
        LOGGER.info("Offline Activity teacher graded flag Inserted into oacs table {} & OA {} ",
            studentId, collectionId);
        return true;
      }
    } else if (duplicateRow != null && oacs.isValid()) {
      long id = Long.valueOf(duplicateRow.get("id").toString());
      int res = Base.exec(AJEntityOACompletionStatus.UPDATE_OA_TEACHER_GRADED_FLAG, true,
            new Timestamp(System.currentTimeMillis()), id);
      if (res > 0) {
        LOGGER.info("Offline Activity teacher graded flag updated into oacs table {} & OA {} ",
            studentId, collectionId);
        return true;
      } else {
        LOGGER.error(
            "Offline Activity teacher graded flag cannot be updated into oacs table {} & OA {} ",
            studentId, collectionId);
        return false;
      }
    } else { // catchAll
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
    RDAEventDispatcher rdaEventDispatcher = null;
    if (contentSource.equalsIgnoreCase(EventConstants.DCA)) {
      rdaEventDispatcher = new RDAEventDispatcher(this.rubricGrading, collectionType,
          scoreInPercent, maxScore, pathId, pathType, null, null, true);
    } else {
      this.rubricGrading.set(AJEntityReporting.SESSION_ID, this.sessionId);
      rdaEventDispatcher = new RDAEventDispatcher(this.rubricGrading, collectionType,
          scoreInPercent, maxScore, pathId, pathType, null, null, true);
    }
    rdaEventDispatcher.sendOATeacherGradeEventFromOATGHToRDA();
  }

  private void sendOAScoreUpdateEventToGEP() {
    String additionalContext = null;
    if (contentSource.equalsIgnoreCase(EventConstants.DCA)) {
      additionalContext = setBase64EncodedAdditionalContext();
    } else {
      this.rubricGrading.set(AJEntityReporting.SESSION_ID, this.sessionId);
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

