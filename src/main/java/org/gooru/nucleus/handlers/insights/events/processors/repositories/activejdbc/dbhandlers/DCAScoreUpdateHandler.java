package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import static org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.validators.ValidationUtils.validateScoreAndMaxScore;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.GEPConstants;
import org.gooru.nucleus.handlers.insights.events.processors.MessageDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.RDAEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.TeacherScoreOverideEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.EntityBuilder;
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
 * Created by renuka@gooru
 */

public class DCAScoreUpdateHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(DCAScoreUpdateHandler.class);
  //TODO: This Kafka Topic name needs to be picked up from config
  public static final String TOPIC_GEP_USAGE_EVENTS = "org.gooru.da.sink.logW.usage.events";
  public static final String TOPIC_NOTIFICATIONS = "notifications";

  private static final String USER_ID_FROM_SESSION = "userIdFromSession";
  private static final String STUDENT_ID = "student_id";
  private static final String RESOURCES = "resources";
  private static final String CORRECT = "correct";
  private static final String CLASS_ID = "class_id";

  private final ProcessorContext context;
  private AJEntityDailyClassActivity dcaReports;
  private String studentId;
  private Double score;
  private Double max_score;
  private Double rawScore;
  private Long pathId;
  private String pathType;
  private String contextCollectionId;
  private String contextCollectionType;
  private Boolean isGraded;
  private Long timeSpent = 0L;
  private String updated_at;
  private String tenantId;
  private String partnerId;
  private String additionalContext;
  private List<JsonObject> lstResources;

  public DCAScoreUpdateHandler(ProcessorContext context) {
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

    if (StringUtil.isNullOrEmpty(context.request().getString(STUDENT_ID)) ||
        StringUtil.isNullOrEmpty(context.request().getString(AJEntityReporting.CLASS_GOORU_OID))
        || StringUtil.isNullOrEmpty(context.request().getString(AJEntityReporting.SESSION_ID))) {
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
    if (context.request().getString(USER_ID_FROM_SESSION) != null) {
      List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER,
          context.request().getString(CLASS_ID),
          context.request().getString(USER_ID_FROM_SESSION));
      if (owner.isEmpty()) {
        LOGGER.warn("User is not a teacher or collaborator");
        return new ExecutionResult<>(
            MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"),
            ExecutionStatus.FAILED);
      }
    }

    JsonArray resources = context.request().getJsonArray(RESOURCES);
    lstResources = IntStream.range(0, resources.size())
        .mapToObj(index -> (JsonObject) resources.getValue(index))
        .collect(Collectors.toList());

    for (JsonObject attr : lstResources) {
        Double score = attr.getValue(AJEntityDailyClassActivity.SCORE) != null ? Double.valueOf(attr.getValue(AJEntityDailyClassActivity.SCORE).toString()) : null;
        Double maxScore = attr.getValue(AJEntityDailyClassActivity.MAX_SCORE) != null ? Double.valueOf(attr.getValue(AJEntityDailyClassActivity.MAX_SCORE).toString()) : null;
        if (!validateScoreAndMaxScore(score, maxScore)) {
            LOGGER.debug("Invalid score/maxscore given for Score Update");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
                ExecutionStatus.FAILED);
        }
    }

    studentId = context.request().getString(STUDENT_ID);
    additionalContext = context.request().getString(AJEntityDailyClassActivity.ADDITIONAL_CONTEXT);

    dcaReports = new AJEntityDailyClassActivity();
    dcaReports.set(AJEntityDailyClassActivity.GOORUUID, studentId);

    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject ctxt2Relay = generateContextToRelay();

    new DefAJEntityDailyClassActivityBuilder()
        .build(dcaReports, ctxt2Relay, AJEntityDailyClassActivity.getConverterRegistry());
    dcaReports.set(AJEntityDailyClassActivity.CONTENT_SOURCE,
        ctxt2Relay.getString(AJEntityDailyClassActivity.CONTENT_SOURCE, null) != null
            ? ctxt2Relay.getString(AJEntityDailyClassActivity.CONTENT_SOURCE)
            : EventConstants.DCA);
    // Update data at resource / question level and Send Events to GEP
    updateResourceLevelDataAndSendEventsToGEP();
    
    // compute score & rawscores based on input values &&
    // update assessment level score based on newly computed value
    computeScoreAndUpdateAssessment();

    // populate other path & context data at dcaReports
    populatePathAndContextInfo();
    
    // send event to let Notifications subssytem know teacher overide flow happened
    sendScoringFlowDoneEventToNotifications();

    // Send Assessment Score update Event if ALL Questions have been graded
    // also set the isGraded flag based on the status
    sendScoringCompleteEventToGEP();

    // one last event to dispatch for LTI side of sub-systems/end points...
    sendScoringCompleteEventToLTIEPs();

    LOGGER.debug("DONE");
    return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(), ExecutionStatus.SUCCESSFUL);
  }

  private static class DefAJEntityDailyClassActivityBuilder implements
      EntityBuilder<AJEntityDailyClassActivity> {
  }


  @Override
  public boolean handlerReadOnly() {
    return false;
  }

  //********************************************************
  //TODO: Move GEP Event Processing to the EventDispatcher
  //********************************************************

  /*
  * updateResourceLevelDataAndSendEventsToGEP: Update data at resource/question level based on input values
  *                                   And then send out events to GEP subsystem for event dump/capture
  */
  private void updateResourceLevelDataAndSendEventsToGEP() {
    for (JsonObject attr : lstResources) {
        Double score = (attr.getValue(AJEntityDailyClassActivity.SCORE) != null) ? 
                       Double.valueOf(attr.getValue(AJEntityDailyClassActivity.SCORE).toString()) : 
                       null;

        Double maxScore = (attr.getValue(AJEntityDailyClassActivity.MAX_SCORE) != null) ? 
                          Double.valueOf(attr.getValue(AJEntityDailyClassActivity.MAX_SCORE).toString()) : 
                          null;
  
        Base.exec(AJEntityDailyClassActivity.UPDATE_QUESTION_SCORE_U, 
                  score, 
                  maxScore, 
                  true,
                  studentId, dcaReports.get(AJEntityDailyClassActivity.CLASS_GOORU_OID),
                  dcaReports.get(AJEntityDailyClassActivity.SESSION_ID),
                  dcaReports.get(AJEntityDailyClassActivity.COLLECTION_OID),
                  attr.getString(AJEntityDailyClassActivity.RESOURCE_ID));
  
        sendResourceScoreUpdateEventToGEP(score, maxScore, attr.getString(AJEntityDailyClassActivity.RESOURCE_ID));
      }
  }

  /*
  * sendResourceScoreUpdateEventToGEP: Send event via Kafka on GEP usage topic
  */
  private void sendResourceScoreUpdateEventToGEP(Double score, Double maxScore, String resourceId) {
    JsonObject result = new JsonObject();
    JsonObject gepEvent = createResourceScoreUpdateEvent(resourceId);
    
    result.put(GEPConstants.SCORE, score);
    result.put(GEPConstants.MAX_SCORE, maxScore);
    gepEvent.put(GEPConstants.RESULT, result);

    try {
      LOGGER.debug("DCA:The Resource Update GEP Event due to Teacher Override is : {} ", gepEvent);

      MessageDispatcher.getInstance().sendGEPEvent2Kafka(TOPIC_GEP_USAGE_EVENTS, gepEvent);

      LOGGER.info("DCA:Successfully dispatched Resource Update GEP Event due to Teacher Override..");
    } catch (Exception e) {
      LOGGER.error("DCA:Error while dispatching Resource Update GEP Event due to Teacher Override ", e);
    }
  }

  /*
  * createResourceScoreUpdateEvent: Helper to create the necessary event packet for resource level event trigger
  */
  private JsonObject createResourceScoreUpdateEvent(String resourceId) {
    JsonObject resEvent = new JsonObject();
    JsonObject context = new JsonObject();

    resEvent.put(GEPConstants.USER_ID, studentId);
    resEvent.put(GEPConstants.ACTIVITY_TIME, System.currentTimeMillis());
    resEvent.put(GEPConstants.EVENT_ID, UUID.randomUUID().toString());
    resEvent.put(GEPConstants.EVENT_NAME, GEPConstants.RES_SCORE_UPDATE_EVENT);
    resEvent.put(GEPConstants.RESOURCE_ID, resourceId);
    resEvent.put(GEPConstants.RESOURCE_TYPE, GEPConstants.QUESTION);

    context.put(GEPConstants.CLASS_ID, dcaReports.get(AJEntityDailyClassActivity.CLASS_GOORU_OID));
    context.put(GEPConstants.COURSE_ID, dcaReports.get(AJEntityDailyClassActivity.COURSE_GOORU_OID));
    context.put(GEPConstants.UNIT_ID, dcaReports.get(AJEntityDailyClassActivity.UNIT_GOORU_OID));
    context.put(GEPConstants.LESSON_ID, dcaReports.get(AJEntityDailyClassActivity.LESSON_GOORU_OID));
    context.put(GEPConstants.COLLECTION_ID, dcaReports.get(AJEntityDailyClassActivity.COLLECTION_OID));
    context.put(GEPConstants.COLLECTION_TYPE, dcaReports.get(AJEntityDailyClassActivity.COLLECTION_TYPE));

    context.put(GEPConstants.CONTENT_SOURCE, GEPConstants.DAILY_CLASS_ACTIVIY);
    context.put(GEPConstants.PATH_TYPE, dcaReports.get(AJEntityDailyClassActivity.PATH_TYPE));
    context.put(GEPConstants.PATH_ID, dcaReports.get(AJEntityDailyClassActivity.PATH_ID));
    context.put(GEPConstants.CONTEXT_COLLECTION_ID, dcaReports.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_ID));
    context.put(GEPConstants.CONTEXT_COLLECTION_TYPE, dcaReports.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_TYPE));

    context.put(GEPConstants.SESSION_ID, dcaReports.get(AJEntityDailyClassActivity.SESSION_ID));

    resEvent.put(GEPConstants.CONTEXT, context);

    return resEvent;
  }

  /*
  * sendScoringCompleteEventToLTIEPs: Send out event to trigger LTI endpoint invoke 
  *                                   to let external systems know of grading completion
  */
  private void sendScoringCompleteEventToLTIEPs() {
    //
    // We need to fetch the C/A Timespent, since the LTI-SBL event structure needs to be the same irrespective of the eventType
    //& the assumption is that duplicate events will be overlayed onto each other and so should include ALL the KPIs
    Object tsObject = Base.firstCell(AJEntityDailyClassActivity.COMPUTE_TIMESPENT,
                                     dcaReports.get(AJEntityDailyClassActivity.COLLECTION_OID),
                                     dcaReports.get(AJEntityDailyClassActivity.SESSION_ID));
    timeSpent = (tsObject != null) ? 
                Long.valueOf(tsObject.toString()) : 
                0L;
    
    new RDAEventDispatcher(this.dcaReports, 
                           this.studentId, 
                           this.score, 
                           this.max_score, 
                           this.isGraded, null).sendCollScoreUpdateEventFromDCAToRDA();
  }

  /*
  * sendScoringFlowDoneEventToNotifications: Send out event to trigger Notifications subsystem of flow done
  */
  private void sendScoringFlowDoneEventToNotifications() {
    TeacherScoreOverideEventDispatcher tsoEventDispatcher = new TeacherScoreOverideEventDispatcher(this.dcaReports);
    tsoEventDispatcher.sendDCATeacherScoreUpdateEventtoNotifications();
  }
   
  /*
  * sendScoringCompleteEventToGEP: If all questions are graded (and have score), send out event to trigger
  *                                notifications system - so user can be alerted of the report readiness
  */
  private void sendScoringCompleteEventToGEP() {
    // If ALL questions in the coll/assessment are graded, trigger notification to indicate 
    // grading completion - so user can be notified
    LazyList<AJEntityDailyClassActivity> allGraded = AJEntityDailyClassActivity.findBySQL(
                                                        AJEntityDailyClassActivity.IS_COLLECTION_GRADED, 
                                                        studentId,
                                                        dcaReports.get(AJEntityDailyClassActivity.SESSION_ID),
                                                        dcaReports.get(AJEntityDailyClassActivity.COLLECTION_OID),
                                                        EventConstants.COLLECTION_RESOURCE_PLAY, 
                                                        EventConstants.STOP, 
                                                        false);
    if (allGraded == null || allGraded.isEmpty()) {
      isGraded = true;

      JsonObject gepEvent = createCollScoreUpdateEvent();
      LOGGER.debug("DCA:The Collection GEP Event is : {} ", gepEvent);

      try {
        // dispatch the event to GEP over Kafka
        MessageDispatcher.getInstance().sendGEPEvent2Kafka(TOPIC_GEP_USAGE_EVENTS, gepEvent);
        LOGGER.info("DCA:Successfully dispatched Collection Perf GEP Event..");
      } catch (Exception e) {
        LOGGER.error("DCA:Error while dispatching Collection Perf GEP Event ", e);
      }
    } else {
      isGraded = false;
    }
  }

  /*
  * createCollScoreUpdateEvent: Helper to create the necessary event packet for collection level event trigger
  */
  private JsonObject createCollScoreUpdateEvent() {
    JsonObject cpEvent = new JsonObject();
    JsonObject context = new JsonObject();
    JsonObject result = new JsonObject();

    // prepare context object first
    context.put(GEPConstants.CLASS_ID, dcaReports.get(AJEntityDailyClassActivity.CLASS_GOORU_OID));
    context.put(GEPConstants.COURSE_ID, dcaReports.get(AJEntityDailyClassActivity.COURSE_GOORU_OID));
    context.put(GEPConstants.UNIT_ID, dcaReports.get(AJEntityDailyClassActivity.UNIT_GOORU_OID));
    context.put(GEPConstants.LESSON_ID, dcaReports.get(AJEntityDailyClassActivity.LESSON_GOORU_OID));
    context.put(GEPConstants.SESSION_ID, dcaReports.get(AJEntityDailyClassActivity.SESSION_ID));    
    context.put(GEPConstants.CONTENT_SOURCE, GEPConstants.DAILY_CLASS_ACTIVIY);
    context.put(GEPConstants.PATH_TYPE, dcaReports.get(AJEntityDailyClassActivity.PATH_TYPE));
    context.put(GEPConstants.PATH_ID, dcaReports.get(AJEntityDailyClassActivity.PATH_ID));
    context.put(GEPConstants.CONTEXT_COLLECTION_ID, dcaReports.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_ID));
    context.put(GEPConstants.CONTEXT_COLLECTION_TYPE, dcaReports.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_TYPE));
    context.put(GEPConstants.ADDITIONAL_CONTEXT, additionalContext);

    cpEvent.put(GEPConstants.USER_ID, studentId);
    // Since EventTime is not included in this event, we will use time during this event generation
    cpEvent.put(GEPConstants.ACTIVITY_TIME, System.currentTimeMillis());
    cpEvent.put(GEPConstants.EVENT_ID, UUID.randomUUID().toString());
    cpEvent.put(GEPConstants.EVENT_NAME, GEPConstants.COLL_SCORE_UPDATE_EVENT);
    cpEvent.put(GEPConstants.COLLECTION_ID, dcaReports.get(AJEntityDailyClassActivity.COLLECTION_OID));
    cpEvent.put(GEPConstants.COLLECTION_TYPE, dcaReports.get(AJEntityDailyClassActivity.COLLECTION_TYPE));

    cpEvent.put(GEPConstants.CONTEXT, context); // context object to send out

    if (score != null && max_score != null && max_score > 0.0) {
      result.put(GEPConstants.SCORE, score);
      result.put(GEPConstants.MAX_SCORE, max_score);
    } else {
      result.putNull(GEPConstants.SCORE);
      result.put(GEPConstants.MAX_SCORE, max_score);
    }

    cpEvent.put(GEPConstants.RESULT, result);

    return cpEvent;
  }

  /*
  * generateContextToRelay: To generate the context object to relay further 
  *                         Constructed from request context as start point
  */
  private JsonObject generateContextToRelay() {
    // copy the request object and do the necessary modifications
    JsonObject relayJson = context.request().copy();

    relayJson.remove(USER_ID_FROM_SESSION);
    relayJson.remove(STUDENT_ID);
    relayJson.remove(RESOURCES);
    relayJson.remove(AJEntityDailyClassActivity.ADDITIONAL_CONTEXT);

    return relayJson;
  }

  /*
  * computeScoreAndUpdateAssessment: Get the score and raw_score values based on data from DB
  *                         compute score based on max_score and raw_score && 
  *                         update Assessment level values for the specific student
  */
  private void computeScoreAndUpdateAssessment() {
    LazyList<AJEntityDailyClassActivity> scoreTS = AJEntityDailyClassActivity
        .findBySQL(AJEntityDailyClassActivity.COMPUTE_ASSESSMENT_SCORE_POST_GRADING_U,
            studentId, dcaReports.get(AJEntityDailyClassActivity.CLASS_GOORU_OID),
            dcaReports.get(AJEntityDailyClassActivity.COLLECTION_OID),
            dcaReports.get(AJEntityDailyClassActivity.SESSION_ID));            
    LOGGER.debug("scoreTS {} ", scoreTS);

    if (scoreTS != null && !scoreTS.isEmpty()) {
        // we expect only one row with two values....the forEach below might be confusing
        // but since the response is a List we iterate this way
        scoreTS.forEach(m -> {
            rawScore = (m.get(AJEntityDailyClassActivity.SCORE) != null) ? Double.valueOf(m.get(AJEntityDailyClassActivity.SCORE).toString()) : null;        
            LOGGER.debug("rawScore {} ", rawScore);

            max_score = (m.get(AJEntityDailyClassActivity.MAX_SCORE) != null) ? Double.valueOf(m.get(AJEntityDailyClassActivity.MAX_SCORE).toString()) : null;
            LOGGER.debug("max_score {} ", max_score);
        });

        if (rawScore != null && max_score != null && max_score > 0.0) {
            score = ((rawScore * 100) / max_score);
            LOGGER.debug("Re-Computed total Assessment score {} ", score);
        }

        Base.exec(AJEntityDailyClassActivity.UPDATE_ASSESSMENT_SCORE_U, 
                    score, 
                    max_score, 
                    studentId,
                    dcaReports.get(AJEntityDailyClassActivity.CLASS_GOORU_OID),
                    dcaReports.get(AJEntityDailyClassActivity.SESSION_ID),
                    dcaReports.get(AJEntityDailyClassActivity.COLLECTION_OID));
        LOGGER.debug("DCA:Total score updated successfully...");
    }
  }

  /*
  * populatePathAndContextInfo: Get Path, Context and tenancy info and popuate DCA Report object
  */
  private void  populatePathAndContextInfo() {
    AJEntityDailyClassActivity pathIdTypeModel = AJEntityDailyClassActivity
        .findFirst("actor_id = ? AND class_id = ? AND course_id = ? AND unit_id = ? "
                + "AND lesson_id = ? AND collection_id = ? AND event_name = 'collection.play' AND event_type = 'stop'",
            dcaReports.get(AJEntityDailyClassActivity.GOORUUID),
            dcaReports.get(AJEntityDailyClassActivity.CLASS_GOORU_OID),
            dcaReports.get(AJEntityDailyClassActivity.COURSE_GOORU_OID),
            dcaReports.get(AJEntityDailyClassActivity.UNIT_GOORU_OID),
            dcaReports.get(AJEntityDailyClassActivity.LESSON_GOORU_OID),
            dcaReports.get(AJEntityDailyClassActivity.COLLECTION_OID));

    if (pathIdTypeModel != null) {
        pathType = (pathIdTypeModel.get(AJEntityDailyClassActivity.PATH_TYPE) != null) ? 
                    pathIdTypeModel.get(AJEntityDailyClassActivity.PATH_TYPE).toString() : 
                    null;

        pathId = (pathIdTypeModel.get(AJEntityDailyClassActivity.PATH_ID) != null) ?
                 Long.valueOf(pathIdTypeModel.get(AJEntityDailyClassActivity.PATH_ID).toString()) : 
                 0L;

        contextCollectionId = (pathIdTypeModel.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_ID) != null) ? 
                              pathIdTypeModel.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_ID).toString() : 
                              null;

        contextCollectionType = (pathIdTypeModel.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_TYPE) != null) ?
                                pathIdTypeModel.get(AJEntityDailyClassActivity.CONTEXT_COLLECTION_TYPE).toString() :
                                null;

        updated_at = pathIdTypeModel.get(AJEntityDailyClassActivity.UPDATE_TIMESTAMP).toString();

        partnerId = (pathIdTypeModel.get(AJEntityDailyClassActivity.PARTNER_ID) != null) ? 
                    pathIdTypeModel.get(AJEntityDailyClassActivity.PARTNER_ID).toString() : 
                    null;

        tenantId = (pathIdTypeModel.get(AJEntityDailyClassActivity.TENANT_ID) != null) ? 
                    pathIdTypeModel.get(AJEntityDailyClassActivity.TENANT_ID).toString() : 
                    null;

        // Set these values into the baseReports Model for data injection into GEP/Notification Events
        dcaReports.set(AJEntityDailyClassActivity.PATH_TYPE, pathType);
        dcaReports.set(AJEntityDailyClassActivity.PATH_ID, pathId);
        dcaReports.set(AJEntityDailyClassActivity.CONTEXT_COLLECTION_ID, contextCollectionId);
        dcaReports.set(AJEntityDailyClassActivity.CONTEXT_COLLECTION_TYPE, contextCollectionType);
        dcaReports.set(AJEntityDailyClassActivity.PARTNER_ID, partnerId);
        dcaReports.set(AJEntityDailyClassActivity.TENANT_ID, tenantId);
    } else { //This Case should NEVER Arise
        dcaReports.set(AJEntityDailyClassActivity.PATH_TYPE, null);
        dcaReports.set(AJEntityDailyClassActivity.PATH_ID, 0L);
        dcaReports.set(AJEntityDailyClassActivity.CONTEXT_COLLECTION_ID, null);
        dcaReports.set(AJEntityDailyClassActivity.CONTEXT_COLLECTION_TYPE, null);
    }
  }

}