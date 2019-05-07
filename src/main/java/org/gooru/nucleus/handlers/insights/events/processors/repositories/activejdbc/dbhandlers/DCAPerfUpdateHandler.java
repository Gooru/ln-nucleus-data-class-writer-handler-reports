package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.GEPConstants;
import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.gooru.nucleus.handlers.insights.events.processors.MessageDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.GEPEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.RDAEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.TeacherScoreOverideEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
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
public class DCAPerfUpdateHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(DCAPerfUpdateHandler.class);
  public static final String TOPIC_GEP_USAGE_EVENTS = "org.gooru.da.sink.logW.usage.events";
  private static final String USER_ID_FROM_SESSION = "userIdFromSession";
  private static final String STUDENT_ID = "student_id";
  private static final String ACTIVITY_DATE = "activity_date";
  private static final String RESOURCES = "resources"; 
  private final ProcessorContext context;
  private JsonObject req;
  private String studentId;
// Currently these variables are not needed, since we are updating only Collection TS using this handler.   
//  private Long pathId;
//  private String pathType;
//  private String contextCollectionId;
//  private String contextCollectionType;
//  private Boolean isGraded;
//  private Long timeSpent = 0L;
//  private String updated_at;
  
  private Long totalResTS = 0L;
//  private Double totalResScore;
//  private Double totalResMaxScore;
//  private Double percentScore = null;
//  private Double asmtMaxScore;
//  private Double rawScore;

  private String tenantId = null;
  private String partnerId = null;
  private String additionalContext;

  private Date activityDate;
  private String sessionId = null;
  Integer questionCount = 0;


  public DCAPerfUpdateHandler(ProcessorContext context) {
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

    //session_id is not mandatory for Collections. It will be inferred from analytics data.
    //For collections, currently the latest session data(timespent) for this date will be updated.
    if (StringUtil.isNullOrEmpty(context.request().getString(STUDENT_ID))
        || StringUtil
            .isNullOrEmpty(context.request().getString(AJEntityDailyClassActivity.CLASS_GOORU_OID))) {
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
//    if (context.request().getString("userIdFromSession") != null) {
//      List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER,
//          context.request().getString("class_id"),
//          context.request().getString("userIdFromSession"));
//      if (owner.isEmpty()) {
//        LOGGER.warn("User is not a teacher or collaborator");
//        return new ExecutionResult<>(
//            MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"),
//            ExecutionStatus.FAILED);
//      }
//    }
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

//  This is the initial Implementation for Updates to Student's performance by the Teacher. This infra should support 
//  updates to Student's Score and timespent for Assessments as well as collections. Currently we support updates to 
//  Student's score in Assessment and Student's timespent in a Collection. The infra should be consolidated, however,
//  currently, this handler will support only updates to Collection timespent. We have a pre-existing handler to support 
//  Assessment Support update.(which should be reconciled with this in near future)
  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    ExecutionResult<MessageResponse> result;
    AJEntityDailyClassActivity dcaModel = new AJEntityDailyClassActivity();
    req = context.request();
    studentId = req.getString(STUDENT_ID);
    JsonArray resources = req.getJsonArray(RESOURCES);
    activityDate = Date.valueOf(req.getString(ACTIVITY_DATE));
    List<JsonObject> resObj = IntStream.range(0, resources.size())
        .mapToObj(index -> (JsonObject) resources.getValue(index)).collect(Collectors.toList());
    additionalContext = req.getString(AJEntityDailyClassActivity.ADDITIONAL_CONTEXT);
    pruneRequest();

    new DefAJEntityDailyClassActivityBuilder().build(dcaModel, req,
        AJEntityDailyClassActivity.getConverterRegistry());
    dcaModel.set(AJEntityDailyClassActivity.GOORUUID, studentId);
    dcaModel.set(AJEntityDailyClassActivity.DATE_IN_TIME_ZONE, activityDate.toString());

    try {
      String collectionType = req.getString(AJEntityDailyClassActivity.COLLECTION_TYPE);
      switch (collectionType) {
        case EventConstants.ASSESSMENT:
          result = updateAssessmentPerf(resObj);
          break;
        case EventConstants.EXTERNAL_ASSESSMENT:
          // result = updateExtAssessmentPerf(resObj);
          result = null;
          break;
        case EventConstants.COLLECTION:
          result = updateCollectionPerf(resObj);
          break;
        case EventConstants.EXTERNAL_COLLECTION:
          // result = updateExtCollectionPerf(resObj);
          result = null;
          break;
        default:
          LOGGER.warn("Invalid collectionType");
          return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
              ExecutionStatus.FAILED);
      }
      
      return result;
      
    } catch (Throwable t) {
      LOGGER.error("exception while processing event", t);
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
          ExecutionStatus.FAILED);
    }
  }

  private static class DefAJEntityDailyClassActivityBuilder
      implements EntityBuilder<AJEntityDailyClassActivity> {
  }


  @Override
  public boolean handlerReadOnly() {
    return false;
  }

  private void pruneRequest() {
    req.remove(USER_ID_FROM_SESSION);
    req.remove(STUDENT_ID);
    req.remove(ACTIVITY_DATE);
    req.remove(RESOURCES);
    req.remove(AJEntityDailyClassActivity.ADDITIONAL_CONTEXT);
  }

  //Update to Student's Assessment score is supported through a separate handler currently.
  private ExecutionResult<MessageResponse>  updateAssessmentPerf(List<JsonObject> resObj) {
    AJEntityDailyClassActivity dcaModel = new AJEntityDailyClassActivity();
    new DefAJEntityDailyClassActivityBuilder().build(dcaModel, req,
        AJEntityDailyClassActivity.getConverterRegistry());
    
    return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(),
        ExecutionStatus.SUCCESSFUL);


  }

  //Update to Student's Collection Timespent is supported currently. 
  //Code related to the Update of Collection Score is commented out.
  private ExecutionResult<MessageResponse> updateCollectionPerf(List<JsonObject> resources) {
    ExecutionResult<MessageResponse> executionResult = updateResourcePerf(resources);
    if (executionResult.continueProcessing()) {
      executionResult = updateCollectionSummary();
    } else {
      return executionResult;
    }
    return executionResult;    
  }


  private ExecutionResult<MessageResponse> updateResourcePerf(List<JsonObject> resources) {
    AJEntityDailyClassActivity dcaModel = new AJEntityDailyClassActivity();
    dcaModel.set(AJEntityDailyClassActivity.GOORUUID, studentId);
    dcaModel.setDateinTZ(activityDate.toString());
    new DefAJEntityDailyClassActivityBuilder().build(dcaModel, req,
        AJEntityDailyClassActivity.getConverterRegistry());
    Long ts = 0L;

    // Since we are in the Collection Update flow, it is expected that minimally we have received
    // some event related to this collection. If NOT we error out.
    AJEntityDailyClassActivity resEventModel = AJEntityDailyClassActivity.findFirst(
        "actor_id = ? AND class_id = ? AND collection_id = ? AND date_in_time_zone = ? "
            + " ORDER by updated_at DESC",
        dcaModel.get(AJEntityDailyClassActivity.GOORUUID),
        dcaModel.get(AJEntityDailyClassActivity.CLASS_GOORU_OID),
        dcaModel.get(AJEntityDailyClassActivity.COLLECTION_OID), activityDate);
    if (resEventModel != null) {
      sessionId = resEventModel.get(AJEntityDailyClassActivity.SESSION_ID) != null
          ? resEventModel.get(AJEntityDailyClassActivity.SESSION_ID).toString()
              : null;
          tenantId = resEventModel.get(AJEntityDailyClassActivity.TENANT_ID) != null
              ? resEventModel.get(AJEntityDailyClassActivity.TENANT_ID).toString()
                  : null;
          dcaModel.set(AJEntityDailyClassActivity.TENANT_ID, tenantId);
          partnerId = resEventModel.get(AJEntityDailyClassActivity.PARTNER_ID) != null
              ? resEventModel.get(AJEntityDailyClassActivity.PARTNER_ID).toString()
                  : null;
              dcaModel.set(AJEntityDailyClassActivity.PARTNER_ID, partnerId);
    }

    if (sessionId == null) {
      LOGGER.error("SessionId cannot be obtained");
      return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
          ExecutionStatus.FAILED);      
    }

    //The Request Payload contains ALL the resources in this collection.
    //Per the requirement, IF the teacher Skips updating any resource AND if the resource has been played
    //by the student, then the student's timespent is retained.
    //However if the student has played the collection partially, then during the timespent update, 
    //timespent assigned by the teacher will be updated for the resources, including to the one's
    //that the student has skipped.For Resources skipped by both student and teacher, the timespent is 0
    for (Object res : resources) {
      JsonObject resource = (JsonObject) res;
      dcaModel.set(AJEntityDailyClassActivity.ID, null);
      dcaModel.set(AJEntityDailyClassActivity.RESOURCE_ID,
          resource.getString(AJEntityDailyClassActivity.RESOURCE_ID));
      String resourceType = resource.getString(AJEntityDailyClassActivity.RESOURCE_TYPE);
      dcaModel.set(AJEntityDailyClassActivity.RESOURCE_TYPE, resourceType);
      dcaModel.set(AJEntityDailyClassActivity.SESSION_ID, sessionId);
      
      AJEntityDailyClassActivity duplicateResEvent = AJEntityDailyClassActivity.findFirst(
          "actor_id = ? AND class_id = ? AND collection_id = ? AND resource_id = ? AND session_id = ? AND "
              + " date_in_time_zone = ? "
              + "AND event_name = 'collection.resource.play' AND event_type = 'stop' ORDER by updated_at DESC",
          dcaModel.get(AJEntityDailyClassActivity.GOORUUID),
          dcaModel.get(AJEntityDailyClassActivity.CLASS_GOORU_OID),
          dcaModel.get(AJEntityDailyClassActivity.COLLECTION_OID),
          dcaModel.get(AJEntityDailyClassActivity.RESOURCE_ID), sessionId, activityDate);

      
      dcaModel.set(AJEntityDailyClassActivity.EVENTNAME, EventConstants.COLLECTION_RESOURCE_PLAY);
      dcaModel.set(AJEntityDailyClassActivity.EVENTTYPE, EventConstants.STOP);
      dcaModel.set(AJEntityDailyClassActivity.CREATE_TIMESTAMP,
          new Timestamp(System.currentTimeMillis()));
      dcaModel.set(AJEntityDailyClassActivity.UPDATE_TIMESTAMP,
          new Timestamp(System.currentTimeMillis()));

      if (resourceType.equalsIgnoreCase(EventConstants.QUESTION)) {
        //Currently only timespent is updated even for questions and not score.
//        updateCollectionQueScore(resource);
        questionCount += 1;
        dcaModel.set(AJEntityDailyClassActivity.QUESTION_TYPE, resource.getString(AJEntityDailyClassActivity.QUESTION_TYPE));
      } else if (resourceType.equalsIgnoreCase(EventConstants.RESOURCE)) {
        dcaModel.set(AJEntityDailyClassActivity.QUESTION_TYPE, EventConstants.UNKNOWN);
      }
      
      long views = 1;
      // Update "reaction" whenever available (future)
      if (resource.containsKey(AJEntityDailyClassActivity.TIMESPENT)
          && resource.getLong(AJEntityDailyClassActivity.TIMESPENT) != null) {
        ts = resource.getLong(AJEntityDailyClassActivity.TIMESPENT);
        totalResTS += ts;
        dcaModel.set(AJEntityDailyClassActivity.TIMESPENT, ts);
      } else {
        dcaModel.set(AJEntityDailyClassActivity.TIMESPENT, 0);
      }
      dcaModel.set(AJEntityDailyClassActivity.VIEWS, views);
      if (dcaModel.hasErrors()) {
        LOGGER.warn("Errors in Updating Class Activity Data");
      }
      LOGGER.info("Inserting collection.resource.play into DCA " + resource.toString());
      if (dcaModel.isValid()) {
        if (duplicateResEvent == null) {          
          if (dcaModel.insert()) {
            LOGGER.info(
                "collection.resource.play event inserted successfully into DCA" + resource.toString());
//            GEPEventDispatcher eventDispatcher = new GEPEventDispatcher(dcaModel, null, null,
//                null, System.currentTimeMillis(), additionalContext);
            //eventDispatcher.sendCRPEventFromDCAOfflinetoGEP();
          } else {
            LOGGER.error("Error while inserting collection.resource.play event into DCA " +
                context.request().toString());
          }
        } else {
          LOGGER.debug(
              "Found duplicate row in DB for this resource {} in the collection {}, updating ..",
              dcaModel.get(AJEntityDailyClassActivity.RESOURCE_ID),
              dcaModel.get(AJEntityDailyClassActivity.COLLECTION_OID));
          Long id = duplicateResEvent.getLong(AJEntityDailyClassActivity.ID);
          // ONLY UPDATE FOR TIMESPENT IS AVAILABLE FROM FRONT_END CURRENTLY
          Base.exec(AJEntityDailyClassActivity.UPDATE_RESOURCE_TS, ts, id);
        }
      } else {
        LOGGER.warn("Event validation error");
        return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
            ExecutionStatus.FAILED);
      }
    }
    return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(),
        ExecutionStatus.CONTINUE_PROCESSING);
  }

  private ExecutionResult<MessageResponse> updateCollectionQueScore(JsonObject resource) {
    // Double resMaxScore = 0.0;
    // Double score = null;
    //
    // questionCount += 1;
    // String answerStatus;
    // score = resource.getDouble(AJEntityDailyClassActivity.SCORE);
    // resMaxScore = resource.getDouble(AJEntityDailyClassActivity.MAX_SCORE);
    // if (score != null) {
    // answerStatus = EventConstants.ATTEMPTED;
    // if ((score.compareTo(0.0) == 0) && (resMaxScore.compareTo(1.0) == 0)) {
    // answerStatus = EventConstants.INCORRECT;
    // dcaModel.set(AJEntityDailyClassActivity.SCORE, score);
    // dcaModel.set(AJEntityDailyClassActivity.MAX_SCORE, resMaxScore);
    // } else if ((score.compareTo(1.0) == 0) && (resMaxScore.compareTo(1.0) == 0)) {
    // answerStatus = EventConstants.CORRECT;
    // dcaModel.set(AJEntityDailyClassActivity.SCORE, score);
    // dcaModel.set(AJEntityDailyClassActivity.MAX_SCORE, resMaxScore);
    // }
    //
    // if (resMaxScore != null) {
    // if ((score.compareTo(100.00) > 0) || (totalResMaxScore.compareTo(100.00) > 0)
    // || (score.compareTo(resMaxScore) > 0) || (score.compareTo(0.00) < 0)
    // || (resMaxScore.compareTo(0.00) < 0) || (resMaxScore.compareTo(0.00) == 0)) {
    // return new ExecutionResult<>(
    // MessageResponseFactory.createInvalidRequestResponse(
    // "Numeric Field Overflow - Invalid Score/Maxscore"),
    // ExecutionResult.ExecutionStatus.FAILED);
    // }
    // if (totalResScore != null) {
    // totalResScore += score;
    // } else {
    // totalResScore = score;
    // }
    //
    // if (totalResMaxScore != null) {
    // totalResMaxScore += resMaxScore;
    // } else {
    // totalResMaxScore = resMaxScore;
    // }
    // }
    // dcaModel.set(AJEntityDailyClassActivity.RESOURCE_ATTEMPT_STATUS, answerStatus);
    // dcaModel.setBoolean(AJEntityDailyClassActivity.IS_GRADED, true);
    // } else {
    // if (resMaxScore.compareTo(1.0) == 0) {
    // answerStatus = EventConstants.INCORRECT;
    // dcaModel.set(AJEntityDailyClassActivity.SCORE, 0.0);
    // dcaModel.set(AJEntityDailyClassActivity.MAX_SCORE, resMaxScore);
    // } else {
    // answerStatus = EventConstants.ATTEMPTED;
    // dcaModel.set(AJEntityDailyClassActivity.SCORE, 0.0);
    // dcaModel.set(AJEntityDailyClassActivity.MAX_SCORE, resMaxScore);
    //
    // }
    // dcaModel.set(AJEntityDailyClassActivity.RESOURCE_ATTEMPT_STATUS, answerStatus);
    // dcaModel.setBoolean(AJEntityDailyClassActivity.IS_GRADED, true);
    //
    // }

    return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(),
        ExecutionStatus.SUCCESSFUL);

  }

  private ExecutionResult<MessageResponse> updateCollectionSummary() {
    AJEntityDailyClassActivity dcaModel = new AJEntityDailyClassActivity();
    new DefAJEntityDailyClassActivityBuilder().build(dcaModel, req,
        AJEntityDailyClassActivity.getConverterRegistry());
    dcaModel.set(AJEntityDailyClassActivity.GOORUUID, studentId);
    dcaModel.set(AJEntityDailyClassActivity.SESSION_ID, sessionId);
    dcaModel.setDateinTZ(activityDate.toString());

    AJEntityDailyClassActivity duplicateCPEvent = AJEntityDailyClassActivity.findFirst(
        "actor_id = ? AND class_id = ? AND collection_id = ? AND session_id = ? AND "
            + " date_in_time_zone = ? AND event_name = 'collection.play' AND event_type = 'stop' "
            + "ORDER by updated_at DESC",
        dcaModel.get(AJEntityDailyClassActivity.GOORUUID),
        dcaModel.get(AJEntityDailyClassActivity.CLASS_GOORU_OID),
        dcaModel.get(AJEntityDailyClassActivity.COLLECTION_OID), sessionId, activityDate);

    LOGGER.debug("Inserting CP event into Reports DB: " + context.request().toString());

    //Score will not be updated
    // if (this.totalResScore != null && this.totalResMaxScore != null
    // && this.totalResMaxScore > 0.0) {
    // percentScore = ((this.totalResScore * 100) / this.totalResMaxScore);
    // LOGGER.debug("Re-Computed total Assessment score {} ", percentScore);
    // }

    // Update "reaction" whenever available (future)
    dcaModel.set(AJEntityDailyClassActivity.TENANT_ID, tenantId);
    dcaModel.set(AJEntityDailyClassActivity.PARTNER_ID, partnerId);
    dcaModel.set(AJEntityDailyClassActivity.VIEWS, 1);
    dcaModel.set(AJEntityDailyClassActivity.TIMESPENT, this.totalResTS);
    // dcaModel.set(AJEntityDailyClassActivity.SCORE, this.percentScore);
    // dcaModel.set(AJEntityDailyClassActivity.MAX_SCORE, this.totalResMaxScore);
    dcaModel.set(AJEntityDailyClassActivity.QUESTION_COUNT, this.questionCount);
    dcaModel.set(AJEntityDailyClassActivity.RESOURCE_TYPE, EventConstants.NA);
    dcaModel.set(AJEntityDailyClassActivity.QUESTION_TYPE, EventConstants.NA);
    dcaModel.set(AJEntityDailyClassActivity.EVENTNAME, EventConstants.COLLECTION_PLAY);
    dcaModel.set(AJEntityDailyClassActivity.EVENTTYPE, EventConstants.STOP);
    dcaModel.set(AJEntityDailyClassActivity.CREATE_TIMESTAMP,
        new Timestamp(System.currentTimeMillis()));
    dcaModel.set(AJEntityDailyClassActivity.UPDATE_TIMESTAMP,
        new Timestamp(System.currentTimeMillis()));

    //Since teacher is all at once, this cannot be a suggestion. path_id, path_type not set here.
    //Currently OE questions will not be UPDATED. So is_graded status WILL NOT BE ALTERED from this
    //flow.
    if (dcaModel.isValid()) {
      if (duplicateCPEvent == null) {
        if (dcaModel.insert()) {
          LOGGER.info("collection.play event successfully updated into DCA" +
              context.request().toString());
          // sendCPEventToGEPAndRDA(dcaModel, ts);
        } else {
          LOGGER.error("Error while inserting collection summary into DCA" +
              context.request().toString());
        }
      } else {
        LOGGER.info("Found duplicate row in DB for this collection {}, updating ..", 
            dcaModel.get(AJEntityDailyClassActivity.COLLECTION_OID));
        Long id = duplicateCPEvent.getLong(AJEntityDailyClassActivity.ID);
        //Only TS is updated currently. score/max_score updates are not sent for the collection
        Base.exec(AJEntityDailyClassActivity.UPDATE_OVERALL_COLLECTION_TS, totalResTS, id);
      }
    } else {
      LOGGER.warn("Event validation error");
      return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(),
          ExecutionStatus.FAILED);
    }

    return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(),
        ExecutionStatus.SUCCESSFUL);

  }

}
