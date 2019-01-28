package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.sql.Timestamp;
import java.util.Map;

import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.exceptions.MessageResponseWrapperException;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
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

public class DCAStudentSelfReportingHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(DCAStudentSelfReportingHandler.class);
  private static final String USER_ID_FROM_SESSION = "userIdFromSession";
  private static final String EXT_COLLECTION_ID = "external_collection_id";
  private static final String USER_ID = "user_id";
  private static final String PERCENT_SCORE = "percent_score";
  private static final String SCORE = "score";
  private static final String MAX_SCORE = "max_score";
  private static final String EVIDENCE = "evidence";
  private static final String TIME_SPENT = "time_spent";
  private final ProcessorContext context;
  private AJEntityDailyClassActivity dcaReport;
  private Double score;
  private Double percentScore;
  private Double rawScore;
  private Double maxScore;
  String localeDate;
  private String extCollectionId;
  private String collectionType;
  private String userId;

  public DCAStudentSelfReportingHandler(ProcessorContext context) {
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

    dcaReport = new AJEntityDailyClassActivity();
    JsonObject req = context.request();
    LazyList<AJEntityDailyClassActivity> duplicateRow = null;

    req.remove(USER_ID_FROM_SESSION);

    long view = 1;
    long timespent = req.getLong(TIME_SPENT) != null ? req.getLong(TIME_SPENT) : 0;
    dcaReport.set(AJEntityDailyClassActivity.GOORUUID, userId);
    dcaReport.set(AJEntityDailyClassActivity.COLLECTION_OID, extCollectionId);
    dcaReport.set(AJEntityDailyClassActivity.VIEWS, view);
    percentScore = (req.getValue(PERCENT_SCORE) != null) ? Double
        .valueOf(req.getValue(PERCENT_SCORE).toString()) : null;
    if (percentScore != null) {
      if ((percentScore.compareTo(100.00) > 0) || (percentScore.compareTo(0.00) < 0)) {
        return new ExecutionResult<>(MessageResponseFactory
            .createInvalidRequestResponse("Numeric Field Overflow - Invalid Percent Score"),
            ExecutionResult.ExecutionStatus.FAILED);
      } else {
        dcaReport.set(AJEntityDailyClassActivity.SCORE, percentScore);
        dcaReport.set(AJEntityDailyClassActivity.MAX_SCORE, 100);
      }
    } else if (req.getValue(SCORE) != null || req.getValue(MAX_SCORE) != null) {
      rawScore = Double.valueOf(req.getValue(SCORE).toString());
      maxScore = Double.valueOf(req.getValue(MAX_SCORE).toString());
      if ((rawScore.compareTo(100.00) > 0) || (maxScore.compareTo(100.00) > 0)
          || (rawScore.compareTo(0.00) < 0) || (maxScore.compareTo(0.00) < 0)
          || (maxScore.compareTo(0.00) == 0)) {
        return new ExecutionResult<>(MessageResponseFactory
            .createInvalidRequestResponse("Numeric Field Overflow - Invalid Fraction Score"),
            ExecutionResult.ExecutionStatus.FAILED);
      }
      score = (rawScore * 100) / maxScore;
      dcaReport.set(AJEntityDailyClassActivity.SCORE, score);
      dcaReport.set(AJEntityDailyClassActivity.MAX_SCORE, maxScore);
    }

    //Remove ALL the values from the Request that needed processing, so that the rest of the values from
    // the request can be mapped to model
    req.remove(MAX_SCORE);
    req.remove(PERCENT_SCORE);
    req.remove(SCORE);
    req.remove(EXT_COLLECTION_ID);
    req.remove(USER_ID);
    req.remove(AJEntityDailyClassActivity.COLLECTION_OID);
    req.remove(EVIDENCE);

    //& set the fields required at base reports for future reporting
    dcaReport.set(AJEntityDailyClassActivity.EVENTNAME, EventConstants.COLLECTION_PLAY);
    dcaReport.set(AJEntityDailyClassActivity.EVENTTYPE, EventConstants.STOP);
    //baseReports.set(AJEntityReporting.EVENT_ID, UUID.randomUUID());

    new DefAJEntityReportingBuilder()
        .build(dcaReport, req, AJEntityDailyClassActivity.getConverterRegistry());
    if (dcaReport.get(AJEntityDailyClassActivity.CLASS_GOORU_OID) == null ||
        dcaReport.get(AJEntityDailyClassActivity.SESSION_ID) == null) {
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
          ExecutionStatus.FAILED);
    }

    long eventTime = System.currentTimeMillis();
    dcaReport.set(AJEntityDailyClassActivity.CREATE_TIMESTAMP, new Timestamp(eventTime));
    dcaReport.set(AJEntityDailyClassActivity.UPDATE_TIMESTAMP, new Timestamp(eventTime));

    if (dcaReport.get(AJEntityDailyClassActivity.TIME_ZONE) != null) {
      String timeZone = dcaReport.get(AJEntityDailyClassActivity.TIME_ZONE).toString();
      localeDate = BaseUtil.UTCToLocale(eventTime, timeZone);

      if (localeDate != null) {
        dcaReport.setDateinTZ(localeDate);
      }
    }

    duplicateRow = AJEntityDailyClassActivity
        .findBySQL(AJEntityDailyClassActivity.CHECK_IF_EXT_ASSESSMENT_SELF_GRADED,
            dcaReport.get(AJEntityDailyClassActivity.GOORUUID),
            dcaReport.get(AJEntityDailyClassActivity.CLASS_GOORU_OID),
            dcaReport.get(AJEntityDailyClassActivity.COLLECTION_OID),
            dcaReport.get(AJEntityDailyClassActivity.SESSION_ID), EventConstants.COLLECTION_PLAY,
            EventConstants.STOP);

    if (duplicateRow == null || duplicateRow.isEmpty()) {
      boolean result = dcaReport.save();

      if (!result) {
        LOGGER.error(
            "ERROR.Student DCA self report for ext-asmt/ext-coll cannot be inserted into the DB: "
                + req);
        if (dcaReport.hasErrors()) {
          Map<String, String> map = dcaReport.errors();
          JsonObject errors = new JsonObject();
          map.forEach(errors::put);
          return new ExecutionResult<>(MessageResponseFactory.createValidationErrorResponse(errors),
              ExecutionResult.ExecutionStatus.FAILED);
        }
      }
      LOGGER.info("Student DCA Self report for ext-asmt/ext-coll stored successfully " + req);
      return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(),
          ExecutionStatus.SUCCESSFUL);
    } else {
      LOGGER.info("Duplicate record exists. Updating the Self reported data ");
      duplicateRow.forEach(dup -> {
        int id = Integer.valueOf(dup.get(AJEntityDailyClassActivity.ID).toString());
        long views = ((dup.get(AJEntityDailyClassActivity.VIEWS) != null ? Long
            .valueOf(dup.get(AJEntityDailyClassActivity.VIEWS).toString()) : 1) + view);
        long ts = ((dup.get(AJEntityDailyClassActivity.TIMESPENT) != null ? Long.valueOf(dup.get(AJEntityDailyClassActivity.TIMESPENT).toString()) : 0) + timespent);
        if (percentScore != null) {
          Base.exec(AJEntityDailyClassActivity.UPDATE_SELF_GRADED_EXT_ASSESSMENT, views, ts,
              percentScore, 100, new Timestamp(eventTime),
              dcaReport.get(AJEntityDailyClassActivity.TIME_ZONE),
              dcaReport.get(AJEntityDailyClassActivity.DATE_IN_TIME_ZONE), id);
        } else {
          Base.exec(AJEntityDailyClassActivity.UPDATE_SELF_GRADED_EXT_ASSESSMENT, views, ts, score,
              maxScore, new Timestamp(eventTime),
              dcaReport.get(AJEntityDailyClassActivity.TIME_ZONE),
              dcaReport.get(AJEntityDailyClassActivity.DATE_IN_TIME_ZONE), id);
        }

      });

      LOGGER.info("Student DCA Self report for ext-asmt/ext-coll stored successfully " + req);
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
  
  private static class DefAJEntityReportingBuilder implements
      EntityBuilder<AJEntityDailyClassActivity> {

  }

  @Override
  public boolean handlerReadOnly() {
    return false;
  }

}