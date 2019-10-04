package org.gooru.nucleus.handlers.insights.events.rda.processor.collection;

import com.hazelcast.util.StringUtil;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.processors.RDAProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityCollectionPerformance;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author renuka@gooru
 */
public class CollectionStartEventRDAHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(CollectionStartEventRDAHandler.class);
  private final RDAProcessorContext context;
  private AJEntityCollectionPerformance collectionReport;
  private CollectionEventParser collectionEvent;
  Double scoreObj;
  Double maxScoreObj;
  Long tsObj;

  public CollectionStartEventRDAHandler(RDAProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("invalid request received");
      return new ExecutionResult<>(MessageResponseFactory
          .createInvalidRequestResponse("Invalid data received to process events"),
          ExecutionStatus.FAILED);
    }

    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    try {
      collectionReport = new AJEntityCollectionPerformance();
      collectionEvent = context.getCollectionEvent();
      LazyList<AJEntityCollectionPerformance> duplicateRow = null;

      collectionReport.set("actor_id", collectionEvent.getUser());
      collectionReport.set("class_id", collectionEvent.getClassId());
      collectionReport.set("course_id", collectionEvent.getCourseId());
      collectionReport.set("unit_id", collectionEvent.getUnitId());
      collectionReport.set("lesson_id", collectionEvent.getLessonId());
      collectionReport.set("session_id", collectionEvent.getSessionId());
      collectionReport.set("collection_type", collectionEvent.getCollectionType());
      collectionReport.set("collection_id", collectionEvent.getCollectionId());

      collectionReport.set("context_collection_id", collectionEvent.getContextCollectionId());
      collectionReport.set("context_collection_type", collectionEvent.getContextCollectionType());
      collectionReport.set("tenant_id", collectionEvent.getTenantId());
      collectionReport.set("question_count", collectionEvent.getQuestionCount());
      collectionReport.set("partner_id", collectionEvent.getPartnerId());
      collectionReport.set("content_source", collectionEvent.getContentSource());

      // pathId = 0L indicates the main Path. We store pathId only for the
      // altPaths
      if (collectionEvent.getPathId() != 0L) {
        collectionReport.set("path_id", collectionEvent.getPathId());
        if (!StringUtil.isNullOrEmpty(collectionEvent.getPathType())) {
          if (collectionReport.isPathTypeValidForContentSource(collectionEvent.getContentSource(),
              collectionEvent.getPathType())) {
            collectionReport.set("path_type", collectionEvent.getPathType());
          } else {
            LOGGER.warn("Invalid Path Type is passed in event : {}", collectionEvent.getPathType());
          }
        }
      }
      collectionReport.set("created_at", new Timestamp(collectionEvent.getActivityTime()));
      collectionReport.set("updated_at", new Timestamp(collectionEvent.getActivityTime()));

      if (collectionEvent.getTimeZone() != null) {
        String timeZone = collectionEvent.getTimeZone();
        collectionReport.set("time_zone", timeZone);
        String localeDate = UTCToLocale(collectionEvent.getActivityTime(), timeZone);

        if (localeDate != null) {
          collectionReport.setDateinTZ(localeDate);
        }
      }
      collectionReport.set("score", collectionEvent.getScore());
      collectionReport.set("views", collectionEvent.getViews());
      collectionReport.set("timespent", collectionEvent.getTimespent());
      collectionReport.set("max_score", collectionEvent.getMaxScore());
      collectionReport.set("is_graded", collectionEvent.getIsGraded());
      collectionReport.set("status", collectionEvent.getStatus());

      duplicateRow = AJEntityCollectionPerformance
          .findBySQL(AJEntityCollectionPerformance.CHECK_DUPLICATE_COLLECTION_EVENT,
              collectionEvent.getUser(), collectionEvent.getSessionId(),
              collectionEvent.getCollectionId());

      if (collectionReport.hasErrors()) {
        LOGGER.warn("errors in creating Base Report");
      }
      LOGGER.debug("Inserting into Reports DB: " + context.request().toString());

      if (collectionReport.isValid()) {
        if (duplicateRow == null || duplicateRow.isEmpty()) {
          if (collectionReport.insert()) {
            LOGGER.info("Record inserted successfully in Reports DB");
          } else {
            LOGGER.error(
                "Error while inserting event into Reports DB: " + context.request().toString());
          }
        } else {
          LOGGER.debug("Found duplicate row in the DB, so updating duplicate row.....");
          duplicateRow.forEach(dup -> {
            int id = Integer.valueOf(dup.get("id").toString());
            long view = (Long.valueOf(dup.get("views").toString()) + collectionEvent.getViews());
            long ts = (Long.valueOf(dup.get("timespent").toString()) + collectionEvent
                .getTimespent());
            long react = collectionEvent.getReaction() != 0 ? collectionEvent.getReaction() : 0;
            Base.exec(AJEntityCollectionPerformance.UPDATE_COLLECTION_METRICS, view, ts,
                collectionEvent.getScore(), collectionEvent.getMaxScore(), react,
                collectionEvent.getIsGraded(), collectionEvent.getStatus(),
                new Timestamp(collectionEvent.getActivityTime()), id);
          });
        }
      } else {
        LOGGER.warn("Event validation error");
        return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
            ExecutionStatus.FAILED);
      }
    } catch (Exception e) {
      LOGGER.error("EXCEPTION::::", e);
    }
    return new ExecutionResult<>(MessageResponseFactory.createCreatedResponse(),
        ExecutionStatus.SUCCESSFUL);

  }

  @Override
  public boolean handlerReadOnly() {
    return false;
  }

  private String UTCToLocale(Long strUtcDate, String timeZone) {

    String strLocaleDate = null;
    try {
      Long epohTime = strUtcDate;
      Date utcDate = new Date(epohTime);

      SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
      simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
      String strUTCDate = simpleDateFormat.format(utcDate);
      simpleDateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));

      strLocaleDate = simpleDateFormat.format(utcDate);

      LOGGER.debug("UTC Date String: " + strUTCDate);
      LOGGER.debug("Locale Date String: " + strLocaleDate);

    } catch (Exception e) {
      LOGGER.error(e.getMessage());
    }

    return strLocaleDate;
  }
}
