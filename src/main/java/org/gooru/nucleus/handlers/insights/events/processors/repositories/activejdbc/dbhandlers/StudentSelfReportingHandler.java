package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import static org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.validators.ValidationUtils.validateScoreAndMaxScore;
import java.sql.Timestamp;
import java.util.Map;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.exceptions.MessageResponseWrapperException;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.RDAEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.StudentSelfReportingEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.EntityBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.utils.BaseUtil;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.util.StringUtil;
import io.vertx.core.json.JsonObject;

public class StudentSelfReportingHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(StudentSelfReportingHandler.class);
  public static final String TOPIC_NOTIFICATIONS = "notifications";
  private static final String USER_ID_FROM_SESSION = "userIdFromSession";
  private static final String EXT_COLLECTION_ID = "external_collection_id";
  private static final String USER_ID = "user_id";
  private static final String PERCENT_SCORE = "percent_score";
  private static final String SCORE = "score";
  private static final String MAX_SCORE = "max_score";
  private static final String EVIDENCE = "evidence";
  private static final String ILACTIVITY = "ILActivity";
  private static final String COURSEMAP = "courseMap";
  private static final String TIME_SPENT = "time_spent";
  private final ProcessorContext context;
  private AJEntityReporting baseReports;
  private Double score;
  private Double percentScore;
  private Double rawScore;
  private Double maxScore;
  private Double finalScore;
  private Double finalMaxScore;
  private Long views;
  private Long eventTime;
  private String collectionType;
  private String userId;
  private String extCollectionId;
  String localeDate;

  public StudentSelfReportingHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    try {
        if (context.request() == null || context.request().isEmpty()) {
          LOGGER.warn("Invalid Data");
          return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse("Invalid Data"),
              ExecutionStatus.FAILED);
        }
        collectionType = context.request().getString(AJEntityDailyClassActivity.COLLECTION_TYPE);
        extCollectionId = context.request().getString(EXT_COLLECTION_ID);
        userId = context.request().getString(USER_ID);
        validatePayload();
    } catch(MessageResponseWrapperException mrwe) {
        return new ExecutionResult<>(mrwe.getMessageResponse(), ExecutionResult.ExecutionStatus.FAILED);
    }
    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    if (context.request().getString("userIdFromSession") != null) {
      if (!context.request().getString("userIdFromSession")
          .equals(context.request().getString(USER_ID))) {
        return new
            ExecutionResult<>(MessageResponseFactory.createForbiddenResponse
            ("Auth Failure"), ExecutionStatus.FAILED);
      }
    } else if (StringUtil.isNullOrEmpty(context.request().getString("userIdFromSession"))) {
      return new
          ExecutionResult<>(MessageResponseFactory.createForbiddenResponse
          ("Auth Failure"), ExecutionStatus.FAILED);
    }
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }


  @Override
  public ExecutionResult<MessageResponse> executeRequest() {

    baseReports = new AJEntityReporting();
    JsonObject req = context.request();
    LazyList<AJEntityReporting> duplicateRow = null;

    req.remove(USER_ID_FROM_SESSION);
    long view = 1;
    long timespent = req.getLong(TIME_SPENT) != null ? req.getLong(TIME_SPENT) : 0;
    baseReports.set(AJEntityReporting.GOORUUID, userId);
    baseReports.set(AJEntityReporting.COLLECTION_OID, extCollectionId);
    baseReports.set(AJEntityDailyClassActivity.VIEWS, view);
    this.views = view;
    percentScore = (req.getValue(PERCENT_SCORE) != null) ? Double
        .valueOf(req.getValue(PERCENT_SCORE).toString()) : null;
    if (percentScore != null) {
      if ((percentScore.compareTo(100.00) > 0) || (percentScore.compareTo(0.00) < 0)) {
        return new ExecutionResult<>(MessageResponseFactory
            .createInvalidRequestResponse("Numeric Field Overflow - Invalid Percent Score"),
            ExecutionResult.ExecutionStatus.FAILED);
      } else {
        baseReports.set(AJEntityReporting.SCORE, percentScore);
        baseReports.set(AJEntityReporting.MAX_SCORE, 100);
        this.finalScore = percentScore;
        this.finalMaxScore = 100.0;
      }
    } else if (req.getValue(AJEntityDailyClassActivity.SCORE) != null
        && req.getValue(AJEntityDailyClassActivity.MAX_SCORE) != null) {
      rawScore = Double.valueOf(req.getValue(SCORE).toString());
      maxScore = Double.valueOf(req.getValue(MAX_SCORE).toString());
      //the value 0 if anotherDouble is numerically equal to this Double;
      //a value less than 0 if this Double is numerically less than anotherDouble;
      //and a value greater than 0 if this Double is numerically greater than anotherDouble.
      if (!validateScoreAndMaxScore(rawScore, maxScore)) {
        return new ExecutionResult<>(MessageResponseFactory
            .createInvalidRequestResponse("Numeric Field Overflow - Invalid Fraction Score"),
            ExecutionResult.ExecutionStatus.FAILED);
      }
      score = (rawScore * 100) / maxScore;
      baseReports.set(AJEntityReporting.SCORE, score);
      baseReports.set(AJEntityReporting.MAX_SCORE, maxScore);
      this.finalScore = score;
      this.finalMaxScore = maxScore;
    }

    //Remove ALL the values from the Request that needed processing, so that the rest of the values from
    // the request can be mapped to model
    req.remove(MAX_SCORE);
    req.remove(PERCENT_SCORE);
    req.remove(SCORE);
    req.remove(EXT_COLLECTION_ID);
    req.remove(USER_ID);
    req.remove(AJEntityReporting.COLLECTION_OID);
    req.remove(EVIDENCE);

    //& set the fields required at base reports for future reporting
    baseReports.set(AJEntityReporting.EVENTNAME, EventConstants.COLLECTION_PLAY);
    baseReports.set(AJEntityReporting.EVENTTYPE, EventConstants.STOP);
    //baseReports.set(AJEntityReporting.EVENT_ID, UUID.randomUUID());

    new DefAJEntityReportingBuilder()
        .build(baseReports, req, AJEntityReporting.getConverterRegistry());
    if (baseReports.get(AJEntityReporting.COURSE_GOORU_OID) == null ||
        baseReports.get(AJEntityReporting.SESSION_ID) == null) {
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
          ExecutionStatus.FAILED);
    } else if (baseReports.get(AJEntityReporting.CLASS_GOORU_OID) == null && !baseReports
        .get(AJEntityReporting.CONTENT_SOURCE).toString().equalsIgnoreCase(ILACTIVITY)) {
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
          ExecutionStatus.FAILED);
    } else if (baseReports.get(AJEntityReporting.CLASS_GOORU_OID) != null && !baseReports
        .get(AJEntityReporting.CONTENT_SOURCE).toString().equalsIgnoreCase(COURSEMAP)) {
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
          ExecutionStatus.FAILED);
    }

    long eventTime = System.currentTimeMillis();
    this.eventTime = eventTime;
    baseReports.set(AJEntityReporting.CREATE_TIMESTAMP, new Timestamp(eventTime));
    baseReports.set(AJEntityReporting.UPDATE_TIMESTAMP, new Timestamp(eventTime));

    if (baseReports.get(AJEntityReporting.TIME_ZONE) != null) {
      String timeZone = baseReports.get(AJEntityReporting.TIME_ZONE).toString();
      localeDate = BaseUtil.UTCToLocale(eventTime, timeZone);

      if (localeDate != null) {
        baseReports.setDateinTZ(localeDate);
      }
    }

    duplicateRow = AJEntityReporting
        .findBySQL(AJEntityReporting.CHECK_IF_EXT_ASSESSMENT_SELF_GRADED,
            baseReports.get(AJEntityReporting.GOORUUID),
            baseReports.get(AJEntityReporting.CLASS_GOORU_OID),
            baseReports.get(AJEntityReporting.COLLECTION_OID),
            baseReports.get(AJEntityReporting.SESSION_ID), EventConstants.COLLECTION_PLAY,
            EventConstants.STOP);

    StudentSelfReportingEventDispatcher eventDispatcher = new StudentSelfReportingEventDispatcher(
        baseReports);
    RDAEventDispatcher rdaEventDispatcher = new RDAEventDispatcher(baseReports, this.views, null,
        req.getLong(TIME_SPENT), this.finalMaxScore, this.finalScore, true, this.eventTime);

    if (duplicateRow == null || duplicateRow.isEmpty()) {
      boolean result = baseReports.save();

      if (!result) {
        LOGGER.error(
            "ERROR.Student self report for ext-asmt/ext-coll cannot be inserted into the DB: " + req);
        if (baseReports.hasErrors()) {
          Map<String, String> map = baseReports.errors();
          JsonObject errors = new JsonObject();
          map.forEach(errors::put);
          return new ExecutionResult<>(MessageResponseFactory.createValidationErrorResponse(errors),
              ExecutionResult.ExecutionStatus.FAILED);
        }
      }
      LOGGER.info("Student Self report for ext-asmt/ext-coll stored successfully " + req);

      eventDispatcher.sendSelfReportEventtoNotifications();
      rdaEventDispatcher.sendSelfGradeEventToRDA();
      return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(),
          ExecutionStatus.SUCCESSFUL);
    } else {
      LOGGER.info("Duplicate record exists. Updating the Self reported data");
      duplicateRow.forEach(dup -> {
        int id = Integer.valueOf(dup.get(AJEntityDailyClassActivity.ID).toString());
        long views = ((dup.get(AJEntityDailyClassActivity.VIEWS) != null ? Long
            .valueOf(dup.get(AJEntityDailyClassActivity.VIEWS).toString()) : 1) + view);
        long ts = ((dup.get(AJEntityDailyClassActivity.TIMESPENT) != null ? Long
            .valueOf(dup.get(AJEntityDailyClassActivity.TIMESPENT).toString()) : 0) + timespent);
        if (percentScore != null) {
          Base.exec(AJEntityReporting.UPDATE_SELF_GRADED_EXT_ASSESSMENT, views, ts, percentScore, 100,
              new Timestamp(eventTime),
              baseReports.get(AJEntityReporting.TIME_ZONE),
              baseReports.get(AJEntityReporting.DATE_IN_TIME_ZONE), id);
          this.finalScore = percentScore;
          this.finalMaxScore = 100.0;
        } else {
          Base.exec(AJEntityReporting.UPDATE_SELF_GRADED_EXT_ASSESSMENT, views, ts, score, maxScore,
              new Timestamp(eventTime),
              baseReports.get(AJEntityReporting.TIME_ZONE),
              baseReports.get(AJEntityReporting.DATE_IN_TIME_ZONE), id);
          this.finalScore = score;
          this.finalMaxScore = maxScore;
        }
        this.views = views;
      });

      LOGGER.info("Student Self report for ext-asmt/ext-coll stored successfully " + req);
      eventDispatcher.sendSelfReportEventtoNotifications();
      rdaEventDispatcher = new RDAEventDispatcher(baseReports, this.views, null,
          req.getLong(TIME_SPENT), this.finalMaxScore, this.finalScore, true, this.eventTime);
      rdaEventDispatcher.sendSelfGradeEventToRDA();
      return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(),
          ExecutionStatus.SUCCESSFUL);

    }
  }

  private void validatePayload() {
      if (StringUtil.isNullOrEmpty(extCollectionId) || StringUtil.isNullOrEmpty(userId) || StringUtil.isNullOrEmpty(collectionType)) {
          throw new MessageResponseWrapperException(
              MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"));
      }
      if (collectionType.equalsIgnoreCase(EventConstants.EXTERNAL_COLLECTION)) {
          long ts = 0;
          if (context.request().getValue(TIME_SPENT) != null) {
              try {
                 ts = context.request().getLong(TIME_SPENT);
              } catch (ClassCastException c) {
                  throw new MessageResponseWrapperException(
                      MessageResponseFactory.createInvalidRequestResponse("Invalid time_spent value in Json Payload."));
              }
          }
          if (ts == 0 || context.request().getValue(TIME_SPENT) == null) {
              throw new MessageResponseWrapperException(
                  MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload. Missing time_spent for " +collectionType));
          }
      }
      
      if ((collectionType.equalsIgnoreCase(EventConstants.EXTERNAL_ASSESSMENT)) 
          && (context.request().getValue(PERCENT_SCORE) == null && (context.request().getValue(SCORE) == null || context.request().getValue(MAX_SCORE) == null))) {
          throw new MessageResponseWrapperException(
            MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload. Required score data missing for " +collectionType));
      }
  }
  
  private static class DefAJEntityReportingBuilder implements EntityBuilder<AJEntityReporting> {

  }


  @Override
  public boolean handlerReadOnly() {
    return false;
  }

}
