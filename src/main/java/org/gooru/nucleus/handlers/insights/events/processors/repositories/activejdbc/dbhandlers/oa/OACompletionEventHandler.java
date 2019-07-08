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
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
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
 * @author renuka
 */
public class OACompletionEventHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(OACompletionEventHandler.class);
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
  private String courseId;
  private String unitId;
  private String lessonId;

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
    courseId = context.request().getString(EventConstants.COURSE_ID);
    unitId = context.request().getString(EventConstants.UNIT_ID);
    lessonId = context.request().getString(EventConstants.LESSON_ID);

    if (!ValidationUtils.isValidUUID(classId) || !ValidationUtils.isValidUUID(oaId)
        || !ValidationUtils.isValidUUID(studentId) || StringUtil.isNullOrEmptyAfterTrim(markedBy)
        || (markedBy != null && !VALID_MARKED_BY_TYPE.matcher(markedBy).matches())
        || StringUtil.isNullOrEmptyAfterTrim(contentSource)
        || (contentSource.equalsIgnoreCase(EventConstants.DCA) && oaDcaId == null)
        || (contentSource.equalsIgnoreCase(EventConstants.COURSEMAP)
            && !(ValidationUtils.isValidUUID(courseId) && ValidationUtils.isValidUUID(unitId)
                && ValidationUtils.isValidUUID(lessonId)))) {
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

    if (contentSource.equalsIgnoreCase(EventConstants.DCA)) {
      AJEntityOASelfGrading oaPerf =
          AJEntityOASelfGrading.findFirst(AJEntityOASelfGrading.GET_CA_OA_PERFORMANCE_FOR_STUDENT,
              oaId, oaDcaId, studentId, classId, contentSource);
      if (oaPerf != null) {
        AJEntityDailyClassActivity dca = new AJEntityDailyClassActivity();
        dca = (AJEntityDailyClassActivity) setCoreReportsModel(dca);
        String localeDate = BaseUtil.UTCDate(ts);
        if (localeDate != null) {
          dca.setDateinTZ(localeDate);
        }
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
        if (!insertOrUpdateDCARecord(studentId, dca)) {
          return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
              ExecutionStatus.FAILED);
        }

      } else {
        AJEntityDailyClassActivity notSelfGradedDca = new AJEntityDailyClassActivity();
        notSelfGradedDca = (AJEntityDailyClassActivity) setCoreReportsModel(notSelfGradedDca);
        String localeDate = BaseUtil.UTCDate(ts);
        if (localeDate != null) {
          notSelfGradedDca.setDateinTZ(localeDate);
        }
        notSelfGradedDca.set(AJEntityDailyClassActivity.GOORUUID, studentId);
        notSelfGradedDca.set(AJEntityDailyClassActivity.TIMESPENT, 0L);
        notSelfGradedDca.set(AJEntityDailyClassActivity.SCORE, null);
        notSelfGradedDca.set(AJEntityDailyClassActivity.MAX_SCORE, null);

        if (!insertOrUpdateDCARecord(studentId, notSelfGradedDca)) {
          return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
              ExecutionStatus.FAILED);
        }
      }
    } else if (contentSource.equalsIgnoreCase(EventConstants.COURSEMAP)) {
      AJEntityOASelfGrading oaPerf =
          AJEntityOASelfGrading.findFirst(AJEntityOASelfGrading.GET_CM_OA_PERFORMANCE_FOR_STUDENT,
              oaId, studentId, classId, courseId, unitId, lessonId, contentSource);
      if (oaPerf != null) {
        AJEntityReporting baseReports = new AJEntityReporting();
        baseReports = (AJEntityReporting) setCoreReportsModel(baseReports);
        String localeDate = BaseUtil.UTCDate(ts);
        if (localeDate != null) {
          baseReports.setDateinTZ(localeDate);
        }
        baseReports.set(AJEntityDailyClassActivity.GOORUUID, studentId);
        baseReports.set(AJEntityDailyClassActivity.TIMESPENT,
            oaPerf.get(AJEntityOASelfGrading.TIMESPENT) != null
                ? Long.valueOf(oaPerf.get(AJEntityOASelfGrading.TIMESPENT).toString())
                : 0L);
        baseReports.set(AJEntityDailyClassActivity.SCORE,
            oaPerf.get(AJEntityOASelfGrading.STUDENT_SCORE) != null
                ? Double.valueOf(oaPerf.get(AJEntityOASelfGrading.STUDENT_SCORE).toString())
                : null);
        baseReports.set(AJEntityDailyClassActivity.MAX_SCORE,
            oaPerf.get(AJEntityOASelfGrading.MAX_SCORE) != null
                ? Double.valueOf(oaPerf.get(AJEntityOASelfGrading.MAX_SCORE).toString())
                : null);
        if (!insertOrUpdateBaseReportsRecord(studentId, baseReports)) {
          return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
              ExecutionStatus.FAILED);
        }
      } else {
        AJEntityReporting notSelfGradedDca = new AJEntityReporting();
        notSelfGradedDca = (AJEntityReporting) setCoreReportsModel(notSelfGradedDca);
        String localeDate = BaseUtil.UTCDate(ts);
        if (localeDate != null) {
          notSelfGradedDca.setDateinTZ(localeDate);
        }
        notSelfGradedDca.set(AJEntityReporting.GOORUUID, studentId);
        notSelfGradedDca.set(AJEntityReporting.TIMESPENT, 0L);
        notSelfGradedDca.set(AJEntityReporting.SCORE, null);
        notSelfGradedDca.set(AJEntityReporting.MAX_SCORE, null);

        if (!insertOrUpdateBaseReportsRecord(studentId, notSelfGradedDca)) {
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

  private boolean insertOrUpdateDCARecord(String studentId, AJEntityDailyClassActivity dca) {
    AJEntityDailyClassActivity duplicateRow = null;
    duplicateRow = AJEntityDailyClassActivity.findFirst(
        "actor_id = ? AND collection_id = ? "
            + "AND dca_content_id = ? AND collection_type = 'offline-activity' "
            + "AND event_name = 'collection.play' " + "AND event_type = 'stop'",
        studentId, oaId, oaDcaId);

    if (duplicateRow == null && dca.isValid()) {
      boolean result = dca.insert();
      if (!result) {
        LOGGER.error("Offline Activity Record cannot be inserted into dca for student {} & OA {} ",
            studentId, oaId);
        if (dca.hasErrors()) {
          Map<String, String> map = dca.errors();
          JsonObject errors = new JsonObject();
          map.forEach(errors::put);
        }
        return false;
      } else {
        LOGGER.info("Offline Activity Record Inserted into dca for student {} & OA {} ", studentId,
            oaId);
        return true;
      }
    } else if (duplicateRow != null && dca.isValid()) {
      long id = Long.valueOf(duplicateRow.get("id").toString());
      int res = Base.exec(AJEntityDailyClassActivity.UPDATE_OA_RECORD_FOR_THIS_STUDENT,
          dca.get(AJEntityDailyClassActivity.TIMESPENT), dca.get(AJEntityDailyClassActivity.SCORE),
          dca.get(AJEntityDailyClassActivity.MAX_SCORE), new Timestamp(System.currentTimeMillis()),
          id);
      if (res > 0) {
        LOGGER.info("Offline Activity Record updated into dca for student {} & OA {} ", studentId,
            oaId);
        return true;
      } else {
        LOGGER.error("Offline Activity Record cannot be updated into dca for student {} & OA {} ",
            studentId, oaId);
        return false;
      }
    } else { // catchAll
      return false;
    }
  }

  private boolean insertOrUpdateBaseReportsRecord(String studentId, AJEntityReporting baseReports) {
    AJEntityReporting duplicateRow = null;
    duplicateRow = AJEntityReporting.findFirst(
        "actor_id = ? AND collection_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? "
            + "AND content_source = ? AND collection_type = 'offline-activity' "
            + "AND event_name = 'collection.play' AND event_type = 'stop'",
        studentId, oaId, courseId, unitId, lessonId, contentSource);

    if (duplicateRow == null && baseReports.isValid()) {
      boolean result = baseReports.insert();
      if (!result) {
        LOGGER.error(
            "Offline Activity Record cannot be inserted into base reports for student {} & OA {} ",
            studentId, oaId);
        if (baseReports.hasErrors()) {
          Map<String, String> map = baseReports.errors();
          JsonObject errors = new JsonObject();
          map.forEach(errors::put);
        }
        return false;
      } else {
        LOGGER.info("Offline Activity Record Inserted into base reports for student {} & OA {} ",
            studentId, oaId);
        return true;
      }
    } else if (duplicateRow != null && baseReports.isValid()) {
      long id = Long.valueOf(duplicateRow.get("id").toString());
      int res = Base.exec(AJEntityReporting.UPDATE_OA_RECORD_FOR_THIS_STUDENT,
          baseReports.get(AJEntityReporting.TIMESPENT), baseReports.get(AJEntityReporting.SCORE),
          baseReports.get(AJEntityReporting.MAX_SCORE), new Timestamp(System.currentTimeMillis()),
          id);
      if (res > 0) {
        LOGGER.info("Offline Activity Record updated into base reports for student {} & OA {} ",
            studentId, oaId);
        return true;
      } else {
        LOGGER.error(
            "Offline Activity Record cannot be updated into base reports for student {} & OA {} ",
            studentId, oaId);
        return false;
      }
    } else { // catchAll
      return false;
    }
  }

  private Model setCoreReportsModel(Model model) {
    long ts = System.currentTimeMillis();
    model.set(AJEntityDailyClassActivity.IS_GRADED, false);
    model.set(AJEntityDailyClassActivity.CLASS_GOORU_OID, classId);
    model.set(AJEntityDailyClassActivity.COLLECTION_OID, oaId);
    model.set(AJEntityDailyClassActivity.COLLECTION_TYPE, EventConstants.OFFLINE_ACTIVITY);
    model.set(AJEntityDailyClassActivity.RESOURCE_TYPE, EventConstants.NA);
    model.set(AJEntityDailyClassActivity.QUESTION_TYPE, EventConstants.NA);
    model.set(AJEntityDailyClassActivity.EVENTNAME, EventConstants.COLLECTION_PLAY);
    model.set(AJEntityDailyClassActivity.EVENTTYPE, EventConstants.STOP);
    model.set(AJEntityDailyClassActivity.CREATE_TIMESTAMP, new Timestamp(ts));
    model.set(AJEntityDailyClassActivity.UPDATE_TIMESTAMP, new Timestamp(ts));
    model.set(AJEntityDailyClassActivity.GRADING_TYPE, EventConstants.TEACHER);
    model.set(AJEntityDailyClassActivity.SESSION_ID, UUID.randomUUID().toString());
    model.set(AJEntityDailyClassActivity.CONTENT_SOURCE, contentSource);
    model.set(AJEntityDailyClassActivity.TIME_ZONE, UTC);
    model.set(AJEntityDailyClassActivity.RESOURCE_ATTEMPT_STATUS, EventConstants.ATTEMPTED);
    if (contentSource.equalsIgnoreCase(EventConstants.DCA)) {
      model.set(AJEntityDailyClassActivity.DCA_CONTENT_ID, oaDcaId);
    } else if (contentSource.equalsIgnoreCase(EventConstants.COURSEMAP)) {
      model.set(AJEntityDailyClassActivity.COURSE_GOORU_OID, courseId);
      model.set(AJEntityDailyClassActivity.UNIT_GOORU_OID, unitId);
      model.set(AJEntityDailyClassActivity.LESSON_GOORU_OID, lessonId);
    }
    String localeDate = BaseUtil.UTCDate(ts);
    if (localeDate != null) {
      ((AJEntityDailyClassActivity) model).setDateinTZ(localeDate);
    }
    pathId = context.request().containsKey(AJEntityDailyClassActivity.PATH_ID)
        ? context.request().getLong(AJEntityDailyClassActivity.PATH_ID)
        : 0L;
    pathType = context.request().containsKey(AJEntityDailyClassActivity.PATH_TYPE)
        ? context.request().getString(AJEntityDailyClassActivity.PATH_TYPE)
        : null;
    model.set(AJEntityDailyClassActivity.PATH_ID, pathId);
    model.set(AJEntityDailyClassActivity.PATH_TYPE, pathType);
    return model;
  }

  private AJEntityOACompletionStatus setOACompletionStatusModel() {
    AJEntityOACompletionStatus oacs = new AJEntityOACompletionStatus();
    oacs.set(AJEntityOACompletionStatus.CLASS_ID, UUID.fromString(classId));
    oacs.set(AJEntityOACompletionStatus.OA_ID, UUID.fromString(oaId));
    oacs.set(AJEntityOACompletionStatus.STUDENT_ID, UUID.fromString(studentId));
    oacs.set(AJEntityOACompletionStatus.COLLECTION_TYPE, EventConstants.OFFLINE_ACTIVITY);
    oacs.set(AJEntityOACompletionStatus.CONTENT_SOURCE, contentSource);
    if (markedBy.equalsIgnoreCase(EventConstants.STUDENT)) {
      oacs.set(AJEntityOACompletionStatus.IS_MARKED_BY_STUDENT, true);
    } else {
      oacs.set(AJEntityOACompletionStatus.IS_MARKED_BY_TEACHER, true);
    }
    if (contentSource.equalsIgnoreCase(EventConstants.DCA)) {
      oacs.set(AJEntityOACompletionStatus.OA_DCA_ID, oaDcaId);
    } else if (contentSource.equalsIgnoreCase(EventConstants.COURSEMAP)) {
      oacs.set(EventConstants.COURSE_ID, UUID.fromString(courseId));
      oacs.set(EventConstants.UNIT_ID, UUID.fromString(unitId));
      oacs.set(EventConstants.LESSON_ID, UUID.fromString(lessonId));
    }
    return oacs;
  }

  private boolean insertOrUpdateCompletionRecord(String studentId,
      AJEntityOACompletionStatus oacs) {
    AJEntityOACompletionStatus duplicateRow = null;
    if (contentSource.equalsIgnoreCase(EventConstants.DCA)) {
      duplicateRow = AJEntityOACompletionStatus.findFirst(
          AJEntityOACompletionStatus.GET_DCA_OA_COMPLETION_STATUS, studentId, oaId, oaDcaId,
          classId, contentSource);
    } else if (contentSource.equalsIgnoreCase(EventConstants.COURSEMAP)) {
      duplicateRow = AJEntityOACompletionStatus.findFirst(
          AJEntityOACompletionStatus.GET_CM_OA_COMPLETION_STATUS, studentId, oaId, classId,
          courseId, unitId, lessonId, contentSource);
    }

    if (duplicateRow == null && oacs.isValid()) {
      boolean result = oacs.insert();
      if (!result) {
        LOGGER.error(
            "Offline Activity Completion status cannot be inserted into oacs table {} & OA {} ",
            studentId, oaId);
        if (oacs.hasErrors()) {
          Map<String, String> map = oacs.errors();
          JsonObject errors = new JsonObject();
          map.forEach(errors::put);
        }
        return false;
      } else {
        LOGGER.info("Offline Activity Completion status Inserted into oacs table {} & OA {} ",
            studentId, oaId);
        return true;
      }
    } else if (duplicateRow != null && oacs.isValid()) {
      long id = Long.valueOf(duplicateRow.get("id").toString());
      int res = 0;
      if (markedBy.equalsIgnoreCase(EventConstants.STUDENT)) {
        res = Base.exec(AJEntityOACompletionStatus.UPDATE_OA_COMPLETION_STATUS_BY_STUDENT, true,
            new Timestamp(System.currentTimeMillis()), id);
      } else {
        res = Base.exec(AJEntityOACompletionStatus.UPDATE_OA_COMPLETION_STATUS_BY_TEACHER, true,
            new Timestamp(System.currentTimeMillis()), id);
      }
      if (res > 0) {
        LOGGER.info("Offline Activity Completion status updated into oacs table {} & OA {} ",
            studentId, oaId);
        return true;
      } else {
        LOGGER.error(
            "Offline Activity Completion status cannot be updated into oacs table {} & OA {} ",
            studentId, oaId);
        return false;
      }
    } else { // catchAll
      return false;
    }
  }

}
