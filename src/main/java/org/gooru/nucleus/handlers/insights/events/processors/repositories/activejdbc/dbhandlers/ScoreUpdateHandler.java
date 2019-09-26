package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.GEPConstants;
import org.gooru.nucleus.handlers.insights.events.processors.MessageDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.LTIEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.RDAEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.TeacherScoreOverideEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
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
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


/**
 * Created by mukul@gooru
 */

public class ScoreUpdateHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScoreUpdateHandler.class);
  //TODO: This Kafka Topic name needs to be picked up from config
  public static final String TOPIC_GEP_USAGE_EVENTS = "org.gooru.da.sink.logW.usage.events";
  public static final String TOPIC_NOTIFICATIONS = "notifications";
  private static final String USER_ID_FROM_SESSION = "userIdFromSession";
  private static final String STUDENT_ID = "student_id";
  private static final String RESOURCES = "resources";
  private static final String CORRECT = "correct";
  private static final String INCORRECT = "incorrect";
  private final ProcessorContext context;
  private AJEntityReporting baseReports;
  private String studentId;
  private Double score;
  private Double max_score;
  private Double rawScore;
  private Long pathId;
  private String pathType;
  private String contextCollectionId;
  private String contextCollectionType;
  private String additionalContext;
  private Boolean isGraded;
  private Long timeSpent = 0L;
  private String updated_at;
  private String tenantId;
  private String partnerId;

  public ScoreUpdateHandler(ProcessorContext context) {
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

    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    if (context.request().getString("userIdFromSession") != null) {
      List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER,
          context.request().getString("class_id"),
          context.request().getString("userIdFromSession"));
      if (owner.isEmpty()) {
        LOGGER.warn("User is not a teacher or collaborator");
        return new
            ExecutionResult<>(
            MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"),
            ExecutionStatus.FAILED);
      }
    }
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {

    baseReports = new AJEntityReporting();
    LazyList<AJEntityReporting> allGraded = null;
    JsonObject req = context.request();
    String teacherId = req.getString(USER_ID_FROM_SESSION);
    req.remove(USER_ID_FROM_SESSION);
    studentId = req.getString(STUDENT_ID);
    req.remove(STUDENT_ID);
    JsonArray resources = req.getJsonArray(RESOURCES);

    List<JsonObject> jObj = IntStream.range(0, resources.size())
        .mapToObj(index -> (JsonObject) resources.getValue(index))
        .collect(Collectors.toList());
    req.remove(RESOURCES);

    new DefAJEntityReportingBuilder()
        .build(baseReports, req, AJEntityReporting.getConverterRegistry());
    baseReports.set(AJEntityReporting.CONTENT_SOURCE,
        (req.containsKey(AJEntityReporting.CONTENT_SOURCE)
            && req.getString(AJEntityReporting.CONTENT_SOURCE) != null)
                ? req.getString(AJEntityReporting.CONTENT_SOURCE)
                : EventConstants.COURSEMAP);
    //TODO: Create a Validator functions to add validations for attributes
    if (baseReports.get(AJEntityReporting.CLASS_GOORU_OID) == null
        || baseReports.get(AJEntityReporting.SESSION_ID) == null
        || baseReports.get(AJEntityReporting.COLLECTION_OID) == null) {

      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
          ExecutionStatus.FAILED);
    }

    baseReports.set(AJEntityReporting.GOORUUID, studentId);
    //Since this NOT a Free-Response Question, the Max_Score can be safely assumed to be 1.0. So no need to update the same.
    jObj.forEach(attr -> {
      String ans_status = attr.getString(AJEntityReporting.RESOURCE_ATTEMPT_STATUS);
      Base.exec(AJEntityReporting.UPDATE_QUESTION_SCORE_U,
          ans_status.equalsIgnoreCase(CORRECT) ? 1.0 : 0.0,
          true, attr.getString(AJEntityReporting.RESOURCE_ATTEMPT_STATUS), studentId,
          baseReports.get(AJEntityReporting.CLASS_GOORU_OID),
          baseReports.get(AJEntityReporting.SESSION_ID),
          baseReports.get(AJEntityReporting.COLLECTION_OID),
          attr.getString(AJEntityReporting.RESOURCE_ID));
    });

    LOGGER.debug("Computing total score...");
    LazyList<AJEntityReporting> scoreTS = AJEntityReporting
        .findBySQL(AJEntityReporting.COMPUTE_ASSESSMENT_SCORE_POST_GRADING_U,
            studentId, baseReports.get(AJEntityReporting.CLASS_GOORU_OID),
            baseReports.get(AJEntityReporting.COLLECTION_OID),
            baseReports.get(AJEntityReporting.SESSION_ID));
    LOGGER.debug("scoreTS {} ", scoreTS);

    if (scoreTS != null && !scoreTS.isEmpty()) {
      scoreTS.forEach(m -> {
        rawScore = (m.get(AJEntityReporting.SCORE) != null ? Double
            .valueOf(m.get(AJEntityReporting.SCORE).toString()) : null);
        LOGGER.debug("rawScore {} ", rawScore);
        max_score = (m.get(AJEntityReporting.MAX_SCORE) != null ? Double
            .valueOf(m.get(AJEntityReporting.MAX_SCORE).toString()) : null);
        LOGGER.debug("max_score {} ", max_score);
      });

      if (rawScore != null && max_score != null && max_score > 0.0) {
        score = ((rawScore * 100) / max_score);
        LOGGER.debug("Re-Computed total Assessment score {} ", score);
      }
    }
    Base.exec(AJEntityReporting.UPDATE_ASSESSMENT_SCORE_U, score, max_score, studentId,
        baseReports.get(AJEntityReporting.CLASS_GOORU_OID),
        baseReports.get(AJEntityReporting.SESSION_ID),
        baseReports.get(AJEntityReporting.COLLECTION_OID));
    LOGGER.debug("Total score updated successfully...");

    AJEntityReporting pathIdTypeModel = AJEntityReporting
        .findFirst("actor_id = ? AND class_id = ? AND course_id = ? AND unit_id = ? "
                + "AND lesson_id = ? AND collection_id = ? AND event_name = 'collection.play' AND event_type = 'stop'",
            baseReports.get(AJEntityReporting.GOORUUID),
            baseReports.get(AJEntityReporting.CLASS_GOORU_OID),
            baseReports.get(AJEntityReporting.COURSE_GOORU_OID),
            baseReports.get(AJEntityReporting.UNIT_GOORU_OID),
            baseReports.get(AJEntityReporting.LESSON_GOORU_OID),
            baseReports.get(AJEntityReporting.COLLECTION_OID));

    if (pathIdTypeModel != null) {
      pathType = pathIdTypeModel.get(AJEntityReporting.PATH_TYPE) != null ? pathIdTypeModel
          .get(AJEntityReporting.PATH_TYPE).toString() : null;
      pathId = pathIdTypeModel.get(AJEntityReporting.PATH_ID) != null ? Long
          .valueOf(pathIdTypeModel.get(AJEntityReporting.PATH_ID).toString()) : 0L;
      contextCollectionId =
          pathIdTypeModel.get(AJEntityReporting.CONTEXT_COLLECTION_ID) != null ? pathIdTypeModel
              .get(AJEntityReporting.CONTEXT_COLLECTION_ID).toString() : null;
      contextCollectionType =
          pathIdTypeModel.get(AJEntityReporting.CONTEXT_COLLECTION_TYPE) != null ? pathIdTypeModel
              .get(AJEntityReporting.CONTEXT_COLLECTION_TYPE).toString() : null;
      //Set these values into the baseReports Model for data injection into GEP/Notification Events
      baseReports.set(AJEntityReporting.PATH_TYPE, pathType);
      baseReports.set(AJEntityReporting.PATH_ID, pathId);
      baseReports.set(AJEntityReporting.CONTEXT_COLLECTION_ID, contextCollectionId);
      baseReports.set(AJEntityReporting.CONTEXT_COLLECTION_TYPE, contextCollectionType);
      updated_at = pathIdTypeModel.get(AJEntityReporting.UPDATE_TIMESTAMP).toString();
      partnerId = pathIdTypeModel.get(AJEntityReporting.PARTNER_ID) != null ? pathIdTypeModel
          .get(AJEntityReporting.PARTNER_ID).toString() : null;
      tenantId = pathIdTypeModel.get(AJEntityReporting.TENANT_ID) != null ? pathIdTypeModel
          .get(AJEntityReporting.TENANT_ID).toString() : null;
      baseReports.set(AJEntityReporting.PARTNER_ID, partnerId);
      baseReports.set(AJEntityReporting.TENANT_ID, tenantId);
    } else { //This Case should NEVER Arise
      baseReports.set(AJEntityReporting.PATH_TYPE, null);
      baseReports.set(AJEntityReporting.PATH_ID, 0L);
      baseReports.set(AJEntityReporting.CONTEXT_COLLECTION_ID, null);
      baseReports.set(AJEntityReporting.CONTEXT_COLLECTION_TYPE, null);
    }

    //Send Score Update Events to GEP
    jObj.forEach(attr -> {
      String ans_status = attr.getString(AJEntityReporting.RESOURCE_ATTEMPT_STATUS);
      sendResourceScoreUpdateEventtoGEP(ans_status, attr.getString(AJEntityReporting.RESOURCE_ID));
    });

    TeacherScoreOverideEventDispatcher eventDispatcher = new TeacherScoreOverideEventDispatcher(
        baseReports);
    eventDispatcher.sendTeacherScoreUpdateEventtoNotifications();
    //Send Assessment Score update Event if ALL Questions have been graded
    allGraded = AJEntityReporting.findBySQL(AJEntityReporting.IS_COLLECTION_GRADED, studentId,
        baseReports.get(AJEntityReporting.SESSION_ID),
        baseReports.get(AJEntityReporting.COLLECTION_OID), EventConstants.COLLECTION_RESOURCE_PLAY,
        EventConstants.STOP, false);
    if (allGraded == null || allGraded.isEmpty()) {
      isGraded = true;
      sendCollScoreUpdateEventtoGEP();
    } else {
      isGraded = false;
    }

    //We need to fetch the C/A Timespent, since the LTI-SBL event structure needs to be the same irrespective of the eventType
    //& the assumption is that duplicate events will be overlayed onto each other and so should include ALL the KPIs
    Object tsObject = Base.firstCell(AJEntityReporting.COMPUTE_TIMESPENT,
        baseReports.get(AJEntityReporting.COLLECTION_OID),
        baseReports.get(AJEntityReporting.SESSION_ID));
    timeSpent = tsObject != null ? Long.valueOf(tsObject.toString()) : 0L;
    LTIEventDispatcher ltiEventDispatcher = new LTIEventDispatcher(baseReports, timeSpent,
        updated_at, rawScore, max_score, score, isGraded);
    ltiEventDispatcher.sendTeacherOverrideEventtoLTI();
    RDAEventDispatcher rdaEventDispatcher = new RDAEventDispatcher(this.baseReports, this.studentId,
        score, max_score, this.isGraded);
    rdaEventDispatcher.sendCollScoreUpdateEventFromSUHToRDA();

    LOGGER.debug("DONE");
    return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(),
        ExecutionStatus.SUCCESSFUL);
  }

  private static class DefAJEntityReportingBuilder implements EntityBuilder<AJEntityReporting> {

  }


  @Override
  public boolean handlerReadOnly() {
    return false;
  }

  //********************************************************
  //TODO: Move GEP Event Processing to the EventDispatcher
  //********************************************************

  private void sendResourceScoreUpdateEventtoGEP(String ansStatus, String resId) {
    JsonObject result = new JsonObject();
    JsonObject gepEvent = createResourceScoreUpdateEvent(resId);

    result.put(GEPConstants.SCORE, ansStatus.equalsIgnoreCase(CORRECT) ? 1.0 : 0.0);
    result.put(GEPConstants.MAX_SCORE, 1.0);

    gepEvent.put(GEPConstants.RESULT, result);

    try {
      LOGGER.debug("The Resource Update GEP Event due to Teacher Override is : {} ", gepEvent);
      MessageDispatcher.getInstance().sendGEPEvent2Kafka(TOPIC_GEP_USAGE_EVENTS, gepEvent);
      LOGGER.info("Successfully dispatched Resource Update GEP Event due to Teacher Override..");
    } catch (Exception e) {
      LOGGER.error("Error while dispatching Resource Update GEP Event due to Teacher Override ", e);
    }
  }

  private JsonObject createResourceScoreUpdateEvent(String rId) {
    JsonObject resEvent = new JsonObject();
    JsonObject context = new JsonObject();

    resEvent.put(GEPConstants.USER_ID, studentId);
    resEvent.put(GEPConstants.ACTIVITY_TIME, System.currentTimeMillis());
    resEvent.put(GEPConstants.EVENT_ID, UUID.randomUUID().toString());
    resEvent.put(GEPConstants.EVENT_NAME, GEPConstants.RES_SCORE_UPDATE_EVENT);
    resEvent.put(GEPConstants.RESOURCE_ID, rId);
    resEvent.put(GEPConstants.RESOURCE_TYPE, GEPConstants.QUESTION);

    context.put(GEPConstants.CLASS_ID, baseReports.get(AJEntityReporting.CLASS_GOORU_OID));
    context.put(GEPConstants.COURSE_ID, baseReports.get(AJEntityReporting.COURSE_GOORU_OID));
    context.put(GEPConstants.UNIT_ID, baseReports.get(AJEntityReporting.UNIT_GOORU_OID));
    context.put(GEPConstants.LESSON_ID, baseReports.get(AJEntityReporting.LESSON_GOORU_OID));
    context.put(GEPConstants.COLLECTION_ID, baseReports.get(AJEntityReporting.COLLECTION_OID));
    context.put(GEPConstants.COLLECTION_TYPE, baseReports.get(AJEntityReporting.COLLECTION_TYPE));
    context.put(GEPConstants.CONTENT_SOURCE, GEPConstants.COURSE_MAP);
    context.put(GEPConstants.PATH_TYPE, baseReports.get(AJEntityReporting.PATH_TYPE));
    context.put(GEPConstants.PATH_ID, baseReports.get(AJEntityReporting.PATH_ID));

    context.put(GEPConstants.CONTEXT_COLLECTION_ID,
        baseReports.get(AJEntityReporting.CONTEXT_COLLECTION_ID));
    context.put(GEPConstants.CONTEXT_COLLECTION_TYPE,
        baseReports.get(AJEntityReporting.CONTEXT_COLLECTION_TYPE));

    context.put(GEPConstants.SESSION_ID, baseReports.get(AJEntityReporting.SESSION_ID));

    resEvent.put(GEPConstants.CONTEXT, context);

    return resEvent;

  }

  private void sendCollScoreUpdateEventtoGEP() {
    JsonObject gepEvent = createCollScoreUpdateEvent();

    try {
      LOGGER.debug("The Collection GEP Event is : {} ", gepEvent);
      MessageDispatcher.getInstance().sendGEPEvent2Kafka(TOPIC_GEP_USAGE_EVENTS, gepEvent);
      LOGGER.info("Successfully dispatched Collection Perf GEP Event..");
    } catch (Exception e) {
      LOGGER.error("Error while dispatching Collection Perf GEP Event ", e);
    }
  }

  private JsonObject createCollScoreUpdateEvent() {
    JsonObject cpEvent = new JsonObject();
    JsonObject context = new JsonObject();
    JsonObject result = new JsonObject();

    cpEvent.put(GEPConstants.USER_ID, studentId);
    //Since EventTime is not included in this event, we will use time during this event generation
    cpEvent.put(GEPConstants.ACTIVITY_TIME, System.currentTimeMillis());
    cpEvent.put(GEPConstants.EVENT_ID, UUID.randomUUID().toString());
    cpEvent.put(GEPConstants.EVENT_NAME, GEPConstants.COLL_SCORE_UPDATE_EVENT);
    cpEvent.put(GEPConstants.COLLECTION_ID, baseReports.get(AJEntityReporting.COLLECTION_OID));
    cpEvent.put(GEPConstants.COLLECTION_TYPE, baseReports.get(AJEntityReporting.COLLECTION_TYPE));

    context.put(GEPConstants.CLASS_ID, baseReports.get(AJEntityReporting.CLASS_GOORU_OID));
    context.put(GEPConstants.COURSE_ID, baseReports.get(AJEntityReporting.COURSE_GOORU_OID));
    context.put(GEPConstants.UNIT_ID, baseReports.get(AJEntityReporting.UNIT_GOORU_OID));
    context.put(GEPConstants.LESSON_ID, baseReports.get(AJEntityReporting.LESSON_GOORU_OID));
    context.put(GEPConstants.SESSION_ID, baseReports.get(AJEntityReporting.SESSION_ID));
    context.put(GEPConstants.CONTENT_SOURCE, GEPConstants.COURSE_MAP);
    context.put(GEPConstants.PATH_TYPE, baseReports.get(AJEntityReporting.PATH_TYPE));
    context.put(GEPConstants.PATH_ID, baseReports.get(AJEntityReporting.PATH_ID));

    context.put(GEPConstants.CONTEXT_COLLECTION_ID,
        baseReports.get(AJEntityReporting.CONTEXT_COLLECTION_ID));
    context.put(GEPConstants.CONTEXT_COLLECTION_TYPE,
        baseReports.get(AJEntityReporting.CONTEXT_COLLECTION_TYPE));
    context.putNull(GEPConstants.ADDITIONAL_CONTEXT);

    cpEvent.put(GEPConstants.CONTEXT, context);

    if (score != null && max_score != null && max_score > 0.0) {
      //score = ((score * 100) / max_score);
      result.put(GEPConstants.SCORE, score);
      result.put(GEPConstants.MAX_SCORE, max_score);
    } else {
      result.putNull(GEPConstants.SCORE);
      result.put(GEPConstants.MAX_SCORE, max_score);
    }

    cpEvent.put(GEPConstants.RESULT, result);

    return cpEvent;

  }

}
