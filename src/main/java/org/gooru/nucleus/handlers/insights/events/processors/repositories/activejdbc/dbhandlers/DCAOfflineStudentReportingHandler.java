package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.GEPEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.RDAEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
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
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author renuka@gooru
 */
public class DCAOfflineStudentReportingHandler implements DBHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DCAOfflineStudentReportingHandler.class);
  private static final String USER_ID_FROM_SESSION = "userIdFromSession";
  private static final String STUDENT_ID = "student_id";
  private static final String PERCENT_SCORE = "percent_score";
  private static final String EVIDENCE = "evidence";
  private static final String RESOURCES = "resources";
  private static final String ANSWER_STATUS = "answer_status";
  private final ProcessorContext context;
  private AJEntityDailyClassActivity dcaReport;
  private Double finalScore;
  private Double finalMaxScore;
  private Integer questionCount;
  private String localeDate;
  private Long views = 1L;
  private Long reaction = 0L;
  private Long totalResTS = 0L;
  private Double totalResScore;
  private Double totalResMaxScore;
  private String userId;
  private JsonArray userIds;
  private Boolean isGraded;
  private String collectionType;
  private SimpleDateFormat DATE_FORMAT_YMD = new SimpleDateFormat("yyyy-MM-dd");
  private Boolean isMasteryContributingEvent = false;
  private String additionalContext;

  public DCAOfflineStudentReportingHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("Invalid Data");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse("Invalid Data"),
          ExecutionStatus.FAILED);
    }
    try {
      userId = context.request().getString(STUDENT_ID);
    } catch (Exception e) {
      userIds = context.request().getJsonArray(STUDENT_ID);
    }
    collectionType = context.request().getString(AJEntityDailyClassActivity.COLLECTION_TYPE);

    if (StringUtil.isNullOrEmptyAfterTrim(
        context.request().getString(AJEntityDailyClassActivity.COLLECTION_OID))
        || StringUtil.isNullOrEmptyAfterTrim(collectionType)
        || !EventConstants.COLLECTION_TYPES.matcher(collectionType).matches()
        || (collectionType.equalsIgnoreCase(EventConstants.EXTERNAL_COLLECTION) && userIds == null)
        || (EventConstants.C_A_EA_TYPES.matcher(collectionType).matches()
            && (StringUtil.isNullOrEmptyAfterTrim(userId) || StringUtil.isNullOrEmptyAfterTrim(
                context.request().getString(AJEntityDailyClassActivity.SESSION_ID))))
        || (EventConstants.C_A_TYPES.matcher(collectionType).matches()
            && (!context.request().containsKey(RESOURCES)
                || (context.request().containsKey(RESOURCES)
                    && (context.request().getValue(RESOURCES) == null
                        || context.request().getJsonArray(RESOURCES).isEmpty()))))) {
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
    if (context.request().getString("userIdFromSession") != null) {
      List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER,
          context.request().getString("class_id"),
          context.request().getString("userIdFromSession"));
      if (owner.isEmpty()) {
        LOGGER.warn("User is not a teacher or collaborator");
        return new ExecutionResult<>(
            MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"),
            ExecutionStatus.FAILED);
      }
    }
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {

    dcaReport = new AJEntityDailyClassActivity();
    LazyList<AJEntityDailyClassActivity> duplicateRow = null;
    JsonObject requestPayload = context.request();
    String collectionId = requestPayload.getString(AJEntityDailyClassActivity.COLLECTION_OID);
    String collectionType = requestPayload.getString(AJEntityDailyClassActivity.COLLECTION_TYPE);

    long ts = System.currentTimeMillis();
    if (requestPayload.getString(AJEntityDailyClassActivity.TIME_ZONE) != null) {
      String timeZone = requestPayload.getString(AJEntityDailyClassActivity.TIME_ZONE);
      if (requestPayload.getString(EventConstants.CONDUCTED_ON) != null) {
        try {
          DATE_FORMAT_YMD.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
          Date date = DATE_FORMAT_YMD.parse(requestPayload.getString(EventConstants.CONDUCTED_ON));
          localeDate = BaseUtil.UTCToLocale(date, timeZone);
        } catch (ParseException e) {
          LOGGER.error("OSRH::Parse exception for conducted_on : {}", e.getMessage());
        }
      } else {
        localeDate = BaseUtil.UTCToLocale(ts, timeZone);
      }
      if (localeDate != null) {
        dcaReport.setDateinTZ(localeDate);
      }
    }
    requestPayload.remove(USER_ID_FROM_SESSION);
    requestPayload.remove(STUDENT_ID);
    requestPayload.remove(EventConstants.CONDUCTED_ON);

    if (requestPayload.containsKey(AJEntityDailyClassActivity.TIMESPENT)
        && requestPayload.getLong(AJEntityDailyClassActivity.TIMESPENT) != null) {
      this.totalResTS = requestPayload.getLong(AJEntityDailyClassActivity.TIMESPENT);
    }
    if (requestPayload.containsKey(AJEntityDailyClassActivity.REACTION)
        && requestPayload.getLong(AJEntityDailyClassActivity.REACTION) != null) {
      this.reaction = requestPayload.getLong(AJEntityDailyClassActivity.REACTION);
    }
    if (requestPayload.containsKey(AJEntityDailyClassActivity.SCORE)
        && requestPayload.getDouble(AJEntityDailyClassActivity.SCORE) != null) {
      this.finalScore = requestPayload.getDouble(AJEntityDailyClassActivity.SCORE);
    }
    if (requestPayload.containsKey(AJEntityDailyClassActivity.MAX_SCORE)
        && requestPayload.getDouble(AJEntityDailyClassActivity.MAX_SCORE) != null) {
      this.finalMaxScore = requestPayload.getDouble(AJEntityDailyClassActivity.MAX_SCORE);
    }
    if (requestPayload.containsKey(AJEntityDailyClassActivity.QUESTION_COUNT)
        && requestPayload.getInteger(AJEntityDailyClassActivity.QUESTION_COUNT) != null) {
      this.questionCount = requestPayload.getInteger(AJEntityDailyClassActivity.QUESTION_COUNT);
    }
    if (requestPayload.containsKey(EventConstants.ADDITIONAL_CONTEXT)) {
      //this key will have Base64 Encoded value, check for the existence to send to gep
      if (!StringUtil.isNullOrEmptyAfterTrim(requestPayload.getString(EventConstants.ADDITIONAL_CONTEXT))) {
        isMasteryContributingEvent = true;
        additionalContext = requestPayload.getString(EventConstants.ADDITIONAL_CONTEXT);
      }
      requestPayload.remove(EventConstants.ADDITIONAL_CONTEXT);
    }

    // Generate and store resource play events
    ExecutionResult<MessageResponse> executionResult =
        processResourcePlayData(requestPayload, collectionId, userId, ts);
    if (executionResult.hasFailed()) {
      return executionResult;
    }
    
    if (collectionType.equalsIgnoreCase(EventConstants.EXTERNAL_ASSESSMENT)
        || collectionType.equalsIgnoreCase(EventConstants.EXTERNAL_COLLECTION)) {
      Double percentScore = (requestPayload.getValue(PERCENT_SCORE) != null)
          ? Double.valueOf(requestPayload.getValue(PERCENT_SCORE).toString())
          : null;
      if (percentScore != null) {
        if ((percentScore.compareTo(100.00) > 0) || (percentScore.compareTo(0.00) < 0)) {
          return new ExecutionResult<>(
              MessageResponseFactory
                  .createInvalidRequestResponse("Numeric Field Overflow - Invalid Percent Score"),
              ExecutionResult.ExecutionStatus.FAILED);
        } else {
          this.finalScore = percentScore;
          this.finalMaxScore = 100.0;
        }
        this.isGraded = true;
      } else if ((collectionType.equalsIgnoreCase(EventConstants.EXTERNAL_ASSESSMENT))
          && (requestPayload.getValue(AJEntityDailyClassActivity.SCORE) == null
              || requestPayload.getValue(AJEntityDailyClassActivity.MAX_SCORE) == null)) {
        return new ExecutionResult<>(
            MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
            ExecutionResult.ExecutionStatus.FAILED);
      } else if (requestPayload.getValue(AJEntityDailyClassActivity.SCORE) != null
          && requestPayload.getValue(AJEntityDailyClassActivity.MAX_SCORE) != null) {
        Double rawScore =
            Double.valueOf(requestPayload.getValue(AJEntityDailyClassActivity.SCORE).toString());
        Double maxScore = Double
            .valueOf(requestPayload.getValue(AJEntityDailyClassActivity.MAX_SCORE).toString());
        Double score = null;
        if ((rawScore.compareTo(100.00) > 0) || (maxScore.compareTo(100.00) > 0)
            || (rawScore.compareTo(0.00) < 0) || (maxScore.compareTo(0.00) < 0)
            || (maxScore.compareTo(0.00) == 0) || rawScore.compareTo(maxScore) > 0) {
          return new ExecutionResult<>(
              MessageResponseFactory
                  .createInvalidRequestResponse("Numeric Field Overflow - Invalid Fraction Score/Maxscore"),
              ExecutionResult.ExecutionStatus.FAILED);
        }
        score = (rawScore * 100) / maxScore;
        this.finalScore = score;
        this.finalMaxScore = maxScore;
        this.isGraded = true;
      }
    } else if (this.totalResScore != null && this.totalResMaxScore != null
        && this.totalResMaxScore > 0.0) {
      // if resource play data is given in request, then use total score
      // of questions
      finalScore = ((this.totalResScore * 100) / this.totalResMaxScore);
      finalMaxScore = this.totalResMaxScore;
      LOGGER.debug("Re-Computed total Assessment score {} ", finalScore);
    }

    dcaReport.set(AJEntityDailyClassActivity.IS_GRADED, this.isGraded);
    dcaReport.set(AJEntityDailyClassActivity.VIEWS, this.views);
    dcaReport.set(AJEntityDailyClassActivity.TIMESPENT, this.totalResTS);
    dcaReport.set(AJEntityDailyClassActivity.REACTION, this.reaction);
    dcaReport.set(AJEntityDailyClassActivity.SCORE, this.finalScore);
    dcaReport.set(AJEntityDailyClassActivity.MAX_SCORE, this.finalMaxScore);
    dcaReport.set(AJEntityDailyClassActivity.QUESTION_COUNT, this.questionCount);
    dcaReport.set(AJEntityDailyClassActivity.GOORUUID, userId);
    dcaReport.set(AJEntityDailyClassActivity.COLLECTION_OID, collectionId);
    dcaReport.set(AJEntityDailyClassActivity.RESOURCE_TYPE, EventConstants.NA);
    dcaReport.set(AJEntityDailyClassActivity.QUESTION_TYPE, EventConstants.NA);
    dcaReport.set(AJEntityDailyClassActivity.EVENTNAME, EventConstants.COLLECTION_PLAY);
    dcaReport.set(AJEntityDailyClassActivity.EVENTTYPE, EventConstants.STOP);
    dcaReport.set(AJEntityDailyClassActivity.CREATE_TIMESTAMP, new Timestamp(ts));
    dcaReport.set(AJEntityDailyClassActivity.UPDATE_TIMESTAMP, new Timestamp(ts));
    if (collectionType.contains(EventConstants.ASSESSMENT)) {
      dcaReport.set(AJEntityDailyClassActivity.GRADING_TYPE, EventConstants.TEACHER);
      dcaReport.set(AJEntityDailyClassActivity.IS_GRADED, this.isGraded);
    }

    // Remove ALL the values from the Request that needed processing, so
    // that the rest of the values from the request can be mapped to model
    removeProcessedFieldsFromPayload(requestPayload);
    new DefAJEntityDailyClassActivityBuilder().build(dcaReport, requestPayload,
        AJEntityDailyClassActivity.getConverterRegistry());
    
    duplicateRow =
        AJEntityDailyClassActivity.findBySQL(AJEntityDailyClassActivity.FIND_COLLECTION_EVENT, userId, 
            dcaReport.get(AJEntityDailyClassActivity.SESSION_ID),
            dcaReport.get(AJEntityDailyClassActivity.COLLECTION_OID), EventConstants.STOP,
            EventConstants.COLLECTION_PLAY);

    if (dcaReport.hasErrors()) {
      LOGGER.warn("errors in creating Base Report");
    }
    LOGGER.debug("DCAOSRH::Inserting CP event into Reports DB: " + context.request().toString());

    if (dcaReport.isValid()) {
      if (duplicateRow == null || duplicateRow.isEmpty()) {
        // Set timespent for all students of the class
        if (collectionType.equalsIgnoreCase(EventConstants.EXTERNAL_COLLECTION) && userIds != null
            && !userIds.isEmpty()) {
          userIds.forEach(user -> {
            AJEntityDailyClassActivity dcaReport = new AJEntityDailyClassActivity();
            this.dcaReport.toMap().keySet().forEach(key -> {
              dcaReport.set(key, this.dcaReport.get(key));
            });
            dcaReport.set(AJEntityDailyClassActivity.GOORUUID, user.toString());
            dcaReport.set(AJEntityDailyClassActivity.SESSION_ID, UUID.randomUUID().toString());
            if (dcaReport.insert()) {
              LOGGER
                  .info("Offline student record (Ext-Coll) inserted successfully into Reports DB");
              sendCPEventToGEPAndRDA(dcaReport, ts);
            } else {
              LOGGER.error("Error while inserting offline student event into Reports DB: {}",
                  context.request().toString());
            }
          });
        } else if (dcaReport.insert()) {
          LOGGER.info("Offline student record inserted successfully into Reports DB");
          sendCPEventToGEPAndRDA(dcaReport, ts);
        } else {
          LOGGER.error("Error while inserting offline student event into Reports DB: {}",
              context.request().toString());
        }
      } else {
        LOGGER.info("Duplicate record exists. Ignoring offline student score!");
      }
    } else {
      LOGGER.warn("Event validation error");
      return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
          ExecutionStatus.FAILED);
    }

    return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(),
        ExecutionStatus.SUCCESSFUL);

  }

  private void sendCPEventToGEPAndRDA(AJEntityDailyClassActivity dcaReport, long ts) {
    RDAEventDispatcher rdaEventDispatcher =
        new RDAEventDispatcher(dcaReport, this.views, this.reaction, this.totalResTS,
            this.finalMaxScore, this.finalScore, this.isGraded, ts);
    rdaEventDispatcher.sendOfflineStudentReportEventDCAToRDA();
    if (isMasteryContributingEvent) {
      GEPEventDispatcher eventDispatcher = new GEPEventDispatcher(dcaReport, this.totalResTS,
          this.finalMaxScore, this.finalScore, System.currentTimeMillis(), additionalContext);
      eventDispatcher.sendCPEventFromDCAtoGEP();
    }
  }

  private void removeProcessedFieldsFromPayload(JsonObject requestPayload) {
    requestPayload.remove(AJEntityDailyClassActivity.MAX_SCORE);
    requestPayload.remove(PERCENT_SCORE);
    requestPayload.remove(AJEntityDailyClassActivity.SCORE);
    requestPayload.remove(STUDENT_ID);
    requestPayload.remove(AJEntityDailyClassActivity.COLLECTION_OID);
    requestPayload.remove(EVIDENCE);
    if (requestPayload.getJsonArray(RESOURCES) == null
        || (requestPayload.getJsonArray(RESOURCES) != null
            && requestPayload.getJsonArray(RESOURCES).isEmpty())) {
      requestPayload.remove(RESOURCES);
    }
  }

  private ExecutionResult<MessageResponse> processResourcePlayData(JsonObject requestPayload,
      String collectionId, String userId, long ts) {
    if (requestPayload.getJsonArray(RESOURCES) != null
        && !requestPayload.getJsonArray(RESOURCES).isEmpty()) {
      LOGGER.info("DCAOSRH::Processing CRP events..");
      LazyList<AJEntityDailyClassActivity> duplicateRow;
      JsonArray resources = requestPayload.getJsonArray(RESOURCES);
      requestPayload.remove(RESOURCES);
      this.questionCount = 0;
      this.totalResMaxScore = 0.0;
      this.totalResScore = null;
      for (Object res : resources) {
        AJEntityDailyClassActivity dcaReport = new AJEntityDailyClassActivity();
        JsonObject resource = (JsonObject) res;
        duplicateRow =
            AJEntityDailyClassActivity.findBySQL(AJEntityDailyClassActivity.FIND_RESOURCE_EVENT, userId, 
                collectionId, requestPayload.getString(AJEntityDailyClassActivity.SESSION_ID),
                resource.getString(AJEntityDailyClassActivity.RESOURCE_ID), EventConstants.STOP);

        dcaReport.set(AJEntityDailyClassActivity.COLLECTION_OID, collectionId);
        dcaReport.set(AJEntityDailyClassActivity.RESOURCE_ID,
            resource.getString(AJEntityDailyClassActivity.RESOURCE_ID));
        dcaReport.set(AJEntityDailyClassActivity.GOORUUID, userId);
        dcaReport.set(AJEntityDailyClassActivity.EVENTNAME,
            EventConstants.COLLECTION_RESOURCE_PLAY);
        dcaReport.set(AJEntityDailyClassActivity.EVENTTYPE, EventConstants.STOP);

        dcaReport.set(AJEntityDailyClassActivity.CREATE_TIMESTAMP, new Timestamp(ts));
        dcaReport.set(AJEntityDailyClassActivity.UPDATE_TIMESTAMP, new Timestamp(ts));

        if (localeDate != null) {
          dcaReport.setDateinTZ(localeDate);
        }

        String resourceType = resource.getString(AJEntityDailyClassActivity.RESOURCE_TYPE);
        if (resourceType.equalsIgnoreCase(EventConstants.QUESTION)) {
          this.questionCount += 1;
          Double score = resource.getDouble(AJEntityDailyClassActivity.SCORE);
          if (score != null) {
            String answerStatus = EventConstants.ATTEMPTED;
            if (score == 0) {
              answerStatus = EventConstants.INCORRECT;
            } else if (score == 1) {
              answerStatus = EventConstants.CORRECT;
            }

            Double totalResMaxScore = resource.getDouble(AJEntityDailyClassActivity.MAX_SCORE);
            if (totalResMaxScore != null) {
              if ((score.compareTo(100.00) > 0) || (totalResMaxScore.compareTo(100.00) > 0)
                  || (score.compareTo(totalResMaxScore) > 0) || (score.compareTo(0.00) < 0)
                  || (totalResMaxScore.compareTo(0.00) < 0)
                  || (totalResMaxScore.compareTo(0.00) == 0)) {
                return new ExecutionResult<>(
                    MessageResponseFactory.createInvalidRequestResponse(
                        "Numeric Field Overflow - Invalid Score/Maxscore"),
                    ExecutionResult.ExecutionStatus.FAILED);
              }
              if (this.totalResScore != null) {
                this.totalResScore += score;
              } else {
                this.totalResScore = score;
              }

              if (this.totalResMaxScore != null) {
                this.totalResMaxScore += totalResMaxScore;
              } else {
                this.totalResMaxScore = totalResMaxScore;
              }
            }
            dcaReport.set(AJEntityDailyClassActivity.RESOURCE_ATTEMPT_STATUS, answerStatus);
            dcaReport.setBoolean(AJEntityDailyClassActivity.IS_GRADED, true);
          }
        }
        long views = 1;
        int reaction = resource.containsKey(AJEntityDailyClassActivity.REACTION)
            ? resource.getInteger(AJEntityDailyClassActivity.REACTION)
            : 0;
        if (resource.containsKey(AJEntityDailyClassActivity.TIMESPENT)
            && resource.getLong(AJEntityDailyClassActivity.TIMESPENT) != null) {
          this.totalResTS += resource.getLong(AJEntityDailyClassActivity.TIMESPENT);
        }

        dcaReport.set(AJEntityDailyClassActivity.VIEWS, views);
        dcaReport.set(AJEntityDailyClassActivity.REACTION, reaction);
        dcaReport.set(AJEntityDailyClassActivity.GRADING_TYPE, EventConstants.TEACHER);

        resource.remove(ANSWER_STATUS);
        new DefAJEntityDailyClassActivityBuilder().build(dcaReport, requestPayload,
            AJEntityDailyClassActivity.getConverterRegistry());
        new DefAJEntityDailyClassActivityBuilder().build(dcaReport, resource,
            AJEntityDailyClassActivity.getConverterRegistry());
        
        if (resourceType.equalsIgnoreCase(EventConstants.RESOURCE)) {
          dcaReport.set(AJEntityDailyClassActivity.QUESTION_TYPE, EventConstants.UNKNOWN);
        }

        if (dcaReport.hasErrors()) {
          LOGGER.warn("errors in creating Class Activity");
        }
        LOGGER.debug("Inserting collection.resource.play into Reports DB: " + resource.toString());

        if (dcaReport.isValid()) {
          if (duplicateRow == null || duplicateRow.isEmpty()) {
            if (dcaReport.insert()) {
              LOGGER.info(
                  "Offline Student collection.resource.play event inserted successfully in Reports DB");
              if (isMasteryContributingEvent) {
                GEPEventDispatcher eventDispatcher =
                    new GEPEventDispatcher(dcaReport, null, null, null, System.currentTimeMillis(), additionalContext);
                eventDispatcher.sendCRPEventFromDCAtoGEP();
              }
            } else {
              LOGGER.error("Error while inserting offline student crp event into Reports DB: {}",
                  context.request().toString());
            }
          } else {
            LOGGER.debug("Found duplicate row in DB, so ignoring duplicate row.....");
          }
        } else {
          LOGGER.warn("Event validation error");
          return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
              ExecutionStatus.FAILED);
        }
      }
    }
    return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
        ExecutionStatus.CONTINUE_PROCESSING);
  }

  private static class DefAJEntityDailyClassActivityBuilder
      implements EntityBuilder<AJEntityDailyClassActivity> {

  }

  @Override
  public boolean handlerReadOnly() {
    return false;
  }

}
