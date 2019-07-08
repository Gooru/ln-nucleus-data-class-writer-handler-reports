package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher;

import io.vertx.core.json.JsonObject;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.processors.MessageDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityRubricGrading;
import org.gooru.nucleus.handlers.insights.events.rda.processor.collection.CollectionEventConstants;
import org.gooru.nucleus.handlers.insights.events.rda.processor.resource.ResourceEventConstants;
import org.javalite.activejdbc.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author renuka@gooru
 */
public class RDAEventDispatcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(RDAEventDispatcher.class);
  public static final String TOPIC_RDA = "rda";
  private AJEntityReporting baseReports;
  private AJEntityRubricGrading rubricGrading;
  private AJEntityDailyClassActivity dcaReports;
  private Long views;
  private Long reaction;
  private Long timespent;
  private Double maxScore;
  private Double score;
  private Boolean isGraded;
  private String actorId;
  private Long pathId;
  private String pathType;
  private String contextCollectionId;
  private String contextCollectionType;
  private String collectionType;
  private long activityTime;

  public RDAEventDispatcher(AJEntityReporting baseReports, Long views, Long reaction,
      Long timespent, Double maxScore, Double score, Boolean isGraded, long activityTime) {
    this.baseReports = baseReports;
    this.views = views;
    this.reaction = reaction;
    this.timespent = timespent;
    this.maxScore = maxScore;
    this.score = score;
    this.isGraded = isGraded;
    this.activityTime = activityTime;
  }

  public RDAEventDispatcher(AJEntityReporting baseReports, String actorId, Double score,
      Double maxScore, Boolean isGraded) {
    this.baseReports = baseReports;
    this.actorId = actorId;
    this.isGraded = isGraded;
    this.maxScore = maxScore;
    this.score = score;
  }

  public RDAEventDispatcher(AJEntityRubricGrading rubricGrading, String collectionType,
      Double score, Double maxScore, Long pathId, String pathType, String contextCollectionId,
      String contextCollectionType, Boolean isGraded) {
    this.rubricGrading = rubricGrading;
    this.collectionType = collectionType;
    this.pathId = pathId;
    this.pathType = pathType;
    this.isGraded = isGraded;
    this.contextCollectionId = contextCollectionId;
    this.contextCollectionType = contextCollectionType;
    this.maxScore = maxScore;
    this.score = score;
  }

  public RDAEventDispatcher(AJEntityDailyClassActivity dcaReport, Long views, Long reaction,
      Long timespent, Double maxScore, Double score, Boolean isGraded, long activityTime) {
    this.dcaReports = dcaReport;
    this.views = views;
    this.reaction = reaction;
    this.timespent = timespent;
    this.maxScore = maxScore;
    this.score = score;
    this.isGraded = isGraded;
    this.activityTime = activityTime;
  }

  public RDAEventDispatcher(AJEntityDailyClassActivity dcaReport, String actorId, Double score,
      Double maxScore, Boolean isGraded) {
    this.dcaReports = dcaReport;
    this.actorId = actorId;
    this.isGraded = isGraded;
    this.maxScore = maxScore;
    this.score = score;
  }

  public void sendCollectionStartEventToRDA() {
    try {
      JsonObject rdaEvent = createCollectionStartEvent();
      LOGGER.debug("PEH::Collection Start RDA Event : {} ", rdaEvent);
      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_RDA, rdaEvent);
      LOGGER.info("PEH::Successfully dispatched Collection Start RDA Event..");
    } catch (Exception e) {
      LOGGER.error("PEH::Error while dispatching Collection Start RDA Event ", e);
    }
  }

  public void sendCollectionStopEventToRDA() {
    try {
      JsonObject rdaEvent = createCollectionStopEvent();
      LOGGER.debug("PEH::Collection Stop RDA Event : {} ", rdaEvent);
      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_RDA, rdaEvent);
      LOGGER.info("PEH::Successfully dispatched Collection Perf RDA Event..");
    } catch (Exception e) {
      LOGGER.error("PEH::Error while dispatching Collection Perf RDA Event ", e);
    }
  }

  public void sendCollectionResourcePlayEventToRDA() {
    try {
      JsonObject rdaEvent = createCollectionResourcePlayEvent();
      LOGGER.debug("PEH::Collection Resource RDA Event : {} ", rdaEvent);
      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_RDA, rdaEvent);
      LOGGER.info("PEH::Successfully dispatched Collection Resource RDA Event..");
    } catch (Exception e) {
      LOGGER.error("PEH::Error while dispatching Collection Resource RDA Event ", e);
    }
  }

  public void sendCollScoreUpdateEventFromSUHToRDA() {
    try {
      JsonObject rdaEvent = createCollScoreUpdateEventFromBRorDCA(baseReports);
      LOGGER.debug("SUH::The Collection RDA Event is : {} ", rdaEvent);
      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_RDA, rdaEvent);
      LOGGER.info("SUH::Successfully dispatched Collection score update RDA Event..");
    } catch (Exception e) {
      LOGGER.error("SUH::Error while dispatching Collection score update RDA Event ", e);
    }
  }

  public void sendCollScoreUpdateEventFromDCAToRDA() {
    try {
      JsonObject rdaEvent = createCollScoreUpdateEventFromBRorDCA(dcaReports);
      LOGGER.debug("CA::The DCA Collection RDA Event is : {} ", rdaEvent);
      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_RDA, rdaEvent);
      LOGGER.info("CA::Successfully dispatched DCA Collection score update RDA Event..");
    } catch (Exception e) {
      LOGGER.error("CA::Error while dispatching DCA Collection score update RDA Event ", e);
    }
  }

  public void sendCollScoreUpdateEventFromRGHToRDA() {
    try {
      JsonObject rdaEvent = createCollScoreUpdateEventFromRubricGrading(CollectionEventConstants.EventAttributes.COLLECTION_SCORE_UPDATE_EVENT);
      LOGGER.debug("RGH::The Collection RDA Event is : {} ", rdaEvent);
      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_RDA, rdaEvent);
      LOGGER.info("RGH::Successfully dispatched Collection score update RDA Event..");
    } catch (Exception e) {
      LOGGER.error("RGH::Error while dispatching Collection score update RDA Event ", e);
    }
  }

  public void sendSelfGradeEventToRDA() {
    try {
      JsonObject rdaEvent = createStudentSelfGradeEventFromBaseReports();
      LOGGER.debug("PEH::Collection Student Self Grade RDA Event : {} ", rdaEvent);
      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_RDA, rdaEvent);
      LOGGER.info("PEH::Successfully dispatched Student self grade RDA Event..");
    } catch (Exception e) {
      LOGGER.error("PEH::Error while dispatching Student self grade RDA Event ", e);
    }
  }

  public void sendOfflineStudentReportEventToRDA() {
    try {
      JsonObject rdaEvent = createOfflineStudentPerfEvent(baseReports);
      LOGGER.debug("PEH::Collection OfflineStudentPerf RDA Event : {} ", rdaEvent);
      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_RDA, rdaEvent);
      LOGGER.info("PEH::Successfully dispatched Collection OfflineStudentPerf RDA Event..");
    } catch (Exception e) {
      LOGGER.error("PEH::Error while dispatching Collection OfflineStudentPerf RDA Event ", e);
    }
  }

  public void sendOfflineStudentReportEventDCAToRDA() {
    try {
      JsonObject rdaEvent = createOfflineStudentPerfEvent(dcaReports);
      LOGGER.debug("PEH::Collection DCAOfflineStudentPerf RDA Event : {} ", rdaEvent);
      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_RDA, rdaEvent);
      LOGGER.info("PEH::Successfully dispatched Collection DCAOfflineStudentPerf RDA Event..");
    } catch (Exception e) {
      LOGGER.error("PEH::Error while dispatching Collection DCAOfflineStudentPerf RDA Event ", e);
    }
  }
  
  public void sendOATeacherGradeEventFromOATGHToRDA() {
    try {
      JsonObject rdaEvent = createCollScoreUpdateEventFromRubricGrading(CollectionEventConstants.EventAttributes.OFFLINE_ACTIVITY_TEACHER_GRADE_EVENT);
      LOGGER.debug("RGH::The OA RDA Event is : {} ", rdaEvent);
      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_RDA, rdaEvent);
      LOGGER.info("RGH::Successfully dispatched OA Teacher Grade RDA Event..");
    } catch (Exception e) {
      LOGGER.error("RGH::Error while dispatching OA Teacher Grade RDA Event ", e);
    }
  }

  private JsonObject createCollectionStartEvent() {
    JsonObject cpEvent = new JsonObject();
    JsonObject context = new JsonObject();
    cpEvent.put(CollectionEventConstants.EventAttributes.EVENT_NAME,
        CollectionEventConstants.EventAttributes.COLLECTION_START_EVENT);
    createCollectionContext(baseReports, cpEvent, context);

    JsonObject result = new JsonObject();
    result.put(CollectionEventConstants.EventAttributes.TIMESPENT, 0);
    if (views != null) {
      result.put(EventConstants.VIEWS, views);
    }
    cpEvent.put(CollectionEventConstants.EventAttributes.RESULT, result);
    return cpEvent;
  }

  private JsonObject createCollectionStopEvent() {
    JsonObject cpEvent = new JsonObject();
    JsonObject context = new JsonObject();
    cpEvent.put(CollectionEventConstants.EventAttributes.EVENT_NAME,
        CollectionEventConstants.EventAttributes.COLLECTION_PERF_EVENT);
    createCollectionContext(baseReports, cpEvent, context);
    JsonObject result = new JsonObject();
    if (views != null) {
      result.put(CollectionEventConstants.EventAttributes.VIEWS, views);
    }
    if (score != null) {
      result.put(CollectionEventConstants.EventAttributes.SCORE, score);
    }
    if (timespent != null) {
      result.put(CollectionEventConstants.EventAttributes.TIMESPENT, timespent);
    }
    if (maxScore != null) {
      result.put(CollectionEventConstants.EventAttributes.MAX_SCORE, maxScore);
    }
    if (reaction != null) {
      result.put(CollectionEventConstants.EventAttributes.REACTION, reaction);
    }
    if (isGraded != null) {
      result.put(CollectionEventConstants.EventAttributes.IS_GRADED, isGraded);
    }
    cpEvent.put(CollectionEventConstants.EventAttributes.RESULT, result);
    return cpEvent;
  }

  private JsonObject createCollectionResourcePlayEvent() {
    JsonObject resEvent = new JsonObject();
    JsonObject context = new JsonObject();

    resEvent.put(CollectionEventConstants.EventAttributes.USER_ID,
        baseReports.get(AJEntityReporting.GOORUUID));
    resEvent.put(CollectionEventConstants.EventAttributes.ACTIVITY_TIME, this.activityTime);
    resEvent.put(CollectionEventConstants.EventAttributes.EVENT_NAME,
        ResourceEventConstants.EventAttributes.RESOURCE_PERF_EVENT);
    resEvent.put(CollectionEventConstants.EventAttributes.RESOURCE_ID,
        baseReports.get(AJEntityReporting.RESOURCE_ID));
    resEvent.put(CollectionEventConstants.EventAttributes.RESOURCE_TYPE,
        baseReports.get(AJEntityReporting.RESOURCE_TYPE));

    context.put(CollectionEventConstants.EventAttributes.CLASS_ID,
        baseReports.get(AJEntityReporting.CLASS_GOORU_OID));
    context.put(CollectionEventConstants.EventAttributes.COURSE_ID,
        baseReports.get(AJEntityReporting.COURSE_GOORU_OID));
    context.put(CollectionEventConstants.EventAttributes.UNIT_ID,
        baseReports.get(AJEntityReporting.UNIT_GOORU_OID));
    context.put(CollectionEventConstants.EventAttributes.LESSON_ID,
        baseReports.get(AJEntityReporting.LESSON_GOORU_OID));
    context.put(CollectionEventConstants.EventAttributes.COLLECTION_ID,
        baseReports.get(AJEntityReporting.COLLECTION_OID));
    context.put(CollectionEventConstants.EventAttributes.COLLECTION_TYPE,
        baseReports.get(AJEntityReporting.COLLECTION_TYPE));
    context.put(CollectionEventConstants.EventAttributes.CONTEXT_COLLECTION_ID,
        baseReports.get(AJEntityReporting.CONTEXT_COLLECTION_ID));
    context.put(CollectionEventConstants.EventAttributes.CONTEXT_COLLECTION_TYPE,
        baseReports.get(AJEntityReporting.CONTEXT_COLLECTION_TYPE));
    context.put(CollectionEventConstants.EventAttributes.PATH_ID,
        baseReports.get(AJEntityReporting.PATH_ID));
    context.put(CollectionEventConstants.EventAttributes.SESSION_ID,
        baseReports.get(AJEntityReporting.SESSION_ID));
    context.put(CollectionEventConstants.EventAttributes.PARTNER_ID,
        baseReports.get(AJEntityReporting.PARTNER_ID));
    context.put(CollectionEventConstants.EventAttributes.TENANT_ID,
        baseReports.get(AJEntityReporting.TENANT_ID));

    context.put(CollectionEventConstants.EventAttributes.PATH_TYPE,
        baseReports.get(AJEntityReporting.PATH_TYPE));
    resEvent.put(CollectionEventConstants.EventAttributes.CONTEXT, context);
    resEvent.put(CollectionEventConstants.EventAttributes.EVENT_NAME,
        ResourceEventConstants.EventAttributes.RESOURCE_PERF_EVENT);
    JsonObject result = new JsonObject();
    if (timespent != null) {
      result.put(ResourceEventConstants.EventAttributes.TIMESPENT, timespent);
    }
    resEvent.put(ResourceEventConstants.EventAttributes.RESULT, result);
    return resEvent;
  }

  private void createCollectionContext(Model reports, JsonObject cpEvent, JsonObject context) {
    cpEvent.put(CollectionEventConstants.EventAttributes.USER_ID,
        reports.get(AJEntityReporting.GOORUUID));
    cpEvent.put(CollectionEventConstants.EventAttributes.ACTIVITY_TIME, this.activityTime);
    cpEvent.put(CollectionEventConstants.EventAttributes.COLLECTION_ID,
        reports.get(AJEntityReporting.COLLECTION_OID));
    cpEvent.put(CollectionEventConstants.EventAttributes.COLLECTION_TYPE,
        reports.get(AJEntityReporting.COLLECTION_TYPE));
    context.put(CollectionEventConstants.EventAttributes.CONTEXT_COLLECTION_ID,
        reports.get(AJEntityReporting.CONTEXT_COLLECTION_ID));
    context.put(CollectionEventConstants.EventAttributes.CONTEXT_COLLECTION_TYPE,
        reports.get(AJEntityReporting.CONTEXT_COLLECTION_TYPE));
    context.put(CollectionEventConstants.EventAttributes.CONTENT_SOURCE,
        reports.get(AJEntityReporting.CONTENT_SOURCE));

    context.put(CollectionEventConstants.EventAttributes.CLASS_ID,
        reports.get(AJEntityReporting.CLASS_GOORU_OID));
    context.put(CollectionEventConstants.EventAttributes.COURSE_ID,
        reports.get(AJEntityReporting.COURSE_GOORU_OID));
    context.put(CollectionEventConstants.EventAttributes.UNIT_ID,
        reports.get(AJEntityReporting.UNIT_GOORU_OID));
    context.put(CollectionEventConstants.EventAttributes.LESSON_ID,
        reports.get(AJEntityReporting.LESSON_GOORU_OID));
    context.put(CollectionEventConstants.EventAttributes.SESSION_ID,
        reports.get(AJEntityReporting.SESSION_ID));
    context.put(CollectionEventConstants.EventAttributes.PARTNER_ID,
        reports.get(AJEntityReporting.PARTNER_ID));
    context.put(CollectionEventConstants.EventAttributes.QUESTION_COUNT,
        reports.get(AJEntityReporting.QUESTION_COUNT));
    context.put(CollectionEventConstants.EventAttributes.TENANT_ID,
        reports.get(AJEntityReporting.TENANT_ID));

    context.put(CollectionEventConstants.EventAttributes.PATH_TYPE,
        reports.get(AJEntityReporting.PATH_TYPE));
    context.put(CollectionEventConstants.EventAttributes.PATH_ID,
        reports.get(AJEntityReporting.PATH_ID));
    cpEvent.put(CollectionEventConstants.EventAttributes.TIMEZONE,
        reports.get(AJEntityReporting.TIME_ZONE));

    cpEvent.put(CollectionEventConstants.EventAttributes.CONTEXT, context);
  }

  private JsonObject createCollScoreUpdateEventFromBRorDCA(Model reports) {
    JsonObject cpEvent = new JsonObject();
    JsonObject context = new JsonObject();
    JsonObject result = new JsonObject();

    cpEvent.put(CollectionEventConstants.EventAttributes.USER_ID, actorId);
    cpEvent.put(CollectionEventConstants.EventAttributes.ACTIVITY_TIME, System.currentTimeMillis());
    cpEvent.put(CollectionEventConstants.EventAttributes.COLLECTION_ID,
        reports.get(AJEntityReporting.COLLECTION_OID));
    cpEvent.put(CollectionEventConstants.EventAttributes.COLLECTION_TYPE,
        reports.get(AJEntityReporting.COLLECTION_TYPE));
    context.put(CollectionEventConstants.EventAttributes.CONTEXT_COLLECTION_ID,
        reports.get(AJEntityReporting.CONTEXT_COLLECTION_ID));
    context.put(CollectionEventConstants.EventAttributes.CONTEXT_COLLECTION_TYPE,
        reports.get(AJEntityReporting.CONTEXT_COLLECTION_TYPE));

    context.put(CollectionEventConstants.EventAttributes.CLASS_ID,
        reports.get(AJEntityReporting.CLASS_GOORU_OID));
    context.put(CollectionEventConstants.EventAttributes.COURSE_ID,
        reports.get(AJEntityReporting.COURSE_GOORU_OID));
    context.put(CollectionEventConstants.EventAttributes.UNIT_ID,
        reports.get(AJEntityReporting.UNIT_GOORU_OID));
    context.put(CollectionEventConstants.EventAttributes.LESSON_ID,
        reports.get(AJEntityReporting.LESSON_GOORU_OID));
    context.put(CollectionEventConstants.EventAttributes.SESSION_ID,
        reports.get(AJEntityReporting.SESSION_ID));
    context.put(CollectionEventConstants.EventAttributes.CONTENT_SOURCE,
        reports.get(AJEntityReporting.CONTENT_SOURCE));

    context.put(CollectionEventConstants.EventAttributes.PATH_TYPE,
        reports.get(AJEntityReporting.PATH_TYPE));
    context.put(CollectionEventConstants.EventAttributes.PATH_ID,
        reports.get(AJEntityReporting.PATH_ID));

    cpEvent.put(CollectionEventConstants.EventAttributes.CONTEXT, context);
    cpEvent.put(CollectionEventConstants.EventAttributes.EVENT_NAME,
        CollectionEventConstants.EventAttributes.COLLECTION_SCORE_UPDATE_EVENT);

    if (this.isGraded != null) {
      result.put(CollectionEventConstants.EventAttributes.IS_GRADED, this.isGraded);
    }
    if (this.score != null) {
      result.put(CollectionEventConstants.EventAttributes.SCORE, this.score);
    }
    if (this.maxScore != null) {
      result.put(CollectionEventConstants.EventAttributes.MAX_SCORE, this.maxScore);
    }
    cpEvent.put(CollectionEventConstants.EventAttributes.RESULT, result);

    return cpEvent;

  }

  private JsonObject createCollScoreUpdateEventFromRubricGrading(String eventName) {
    JsonObject cpEvent = new JsonObject();
    JsonObject context = new JsonObject();
    JsonObject result = new JsonObject();

    cpEvent.put(CollectionEventConstants.EventAttributes.USER_ID,
        rubricGrading.get(AJEntityRubricGrading.STUDENT_ID));
    cpEvent.put(CollectionEventConstants.EventAttributes.ACTIVITY_TIME, System.currentTimeMillis());
    cpEvent.put(CollectionEventConstants.EventAttributes.EVENT_NAME,
        eventName);
    cpEvent.put(CollectionEventConstants.EventAttributes.COLLECTION_ID,
        rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID));
    cpEvent.put(CollectionEventConstants.EventAttributes.COLLECTION_TYPE, collectionType);

    context.put(CollectionEventConstants.EventAttributes.CONTEXT_COLLECTION_ID,
        contextCollectionId);
    context.put(CollectionEventConstants.EventAttributes.CONTEXT_COLLECTION_TYPE,
        contextCollectionType);
    context.put(CollectionEventConstants.EventAttributes.PATH_ID, pathId);
    context.put(CollectionEventConstants.EventAttributes.PATH_TYPE, pathType);
    context.put(CollectionEventConstants.EventAttributes.CONTENT_SOURCE,
        rubricGrading.get(AJEntityReporting.CONTENT_SOURCE) != null ? rubricGrading.get(AJEntityReporting.CONTENT_SOURCE) : null);

    context.put(CollectionEventConstants.EventAttributes.CLASS_ID,
        rubricGrading.get(AJEntityRubricGrading.CLASS_ID));
    context.put(CollectionEventConstants.EventAttributes.COURSE_ID,
        rubricGrading.get(AJEntityRubricGrading.COURSE_ID));
    context.put(CollectionEventConstants.EventAttributes.UNIT_ID,
        rubricGrading.get(AJEntityRubricGrading.UNIT_ID));
    context.put(CollectionEventConstants.EventAttributes.LESSON_ID,
        rubricGrading.get(AJEntityRubricGrading.LESSON_ID));
    context.put(CollectionEventConstants.EventAttributes.SESSION_ID,
        rubricGrading.get(AJEntityRubricGrading.SESSION_ID));
    cpEvent.put(CollectionEventConstants.EventAttributes.CONTEXT, context);

    if (this.isGraded != null) {
      result.put(CollectionEventConstants.EventAttributes.IS_GRADED, this.isGraded);
    }
    if (this.score != null) {
      result.put(CollectionEventConstants.EventAttributes.SCORE, this.score);
    }
    if (this.maxScore != null) {
      result.put(CollectionEventConstants.EventAttributes.MAX_SCORE, this.maxScore);
    }
    cpEvent.put(CollectionEventConstants.EventAttributes.RESULT, result);

    return cpEvent;

  }

  private JsonObject createStudentSelfGradeEventFromBaseReports() {

    JsonObject cpEvent = new JsonObject();
    JsonObject context = new JsonObject();
    JsonObject result = new JsonObject();

    cpEvent.put(CollectionEventConstants.EventAttributes.USER_ID,
        baseReports.get(AJEntityReporting.GOORUUID));
    cpEvent.put(CollectionEventConstants.EventAttributes.ACTIVITY_TIME, this.activityTime);
    cpEvent.put(CollectionEventConstants.EventAttributes.COLLECTION_ID,
        baseReports.get(AJEntityReporting.COLLECTION_OID));
    cpEvent.put(CollectionEventConstants.EventAttributes.COLLECTION_TYPE,
        baseReports.get(AJEntityReporting.COLLECTION_TYPE));
    context.put(CollectionEventConstants.EventAttributes.CONTEXT_COLLECTION_ID,
        baseReports.get(AJEntityReporting.CONTEXT_COLLECTION_ID));
    context.put(CollectionEventConstants.EventAttributes.CONTEXT_COLLECTION_TYPE,
        baseReports.get(AJEntityReporting.CONTEXT_COLLECTION_TYPE));
    context.put(CollectionEventConstants.EventAttributes.CONTENT_SOURCE,
        baseReports.get(AJEntityReporting.CONTENT_SOURCE));

    context.put(CollectionEventConstants.EventAttributes.CLASS_ID,
        baseReports.get(AJEntityReporting.CLASS_GOORU_OID));
    context.put(CollectionEventConstants.EventAttributes.COURSE_ID,
        baseReports.get(AJEntityReporting.COURSE_GOORU_OID));
    context.put(CollectionEventConstants.EventAttributes.UNIT_ID,
        baseReports.get(AJEntityReporting.UNIT_GOORU_OID));
    context.put(CollectionEventConstants.EventAttributes.LESSON_ID,
        baseReports.get(AJEntityReporting.LESSON_GOORU_OID));
    context.put(CollectionEventConstants.EventAttributes.SESSION_ID,
        baseReports.get(AJEntityReporting.SESSION_ID));
    context.put(CollectionEventConstants.EventAttributes.PARTNER_ID,
        baseReports.get(AJEntityReporting.PARTNER_ID));
    context.put(CollectionEventConstants.EventAttributes.TENANT_ID,
        baseReports.get(AJEntityReporting.TENANT_ID));

    context.put(CollectionEventConstants.EventAttributes.PATH_TYPE,
        baseReports.get(AJEntityReporting.PATH_TYPE));
    context.put(CollectionEventConstants.EventAttributes.PATH_ID,
        baseReports.get(AJEntityReporting.PATH_ID));

    cpEvent.put(CollectionEventConstants.EventAttributes.CONTEXT, context);
    cpEvent.put(CollectionEventConstants.EventAttributes.EVENT_NAME,
        CollectionEventConstants.EventAttributes.COLLECTION_SELF_GRADE_EVENT);
    cpEvent.put(CollectionEventConstants.EventAttributes.TIMEZONE,
        baseReports.get(AJEntityReporting.TIME_ZONE));

    if (views != null) {
      result.put(CollectionEventConstants.EventAttributes.VIEWS, views);
    }
    if (score != null) {
      result.put(CollectionEventConstants.EventAttributes.SCORE, score);
    }
    if (maxScore != null) {
      result.put(CollectionEventConstants.EventAttributes.MAX_SCORE, maxScore);
    }
    if (timespent != null) {
      result.put(CollectionEventConstants.EventAttributes.TIMESPENT, timespent);
    }
    if (isGraded != null) {
      result.put(CollectionEventConstants.EventAttributes.IS_GRADED, isGraded);
    }
    result.put(CollectionEventConstants.EventAttributes.STATUS, true);
    cpEvent.put(CollectionEventConstants.EventAttributes.RESULT, result);
    return cpEvent;

  }

  private JsonObject createOfflineStudentPerfEvent(Model reports) {
    JsonObject cpEvent = new JsonObject();
    JsonObject context = new JsonObject();
    cpEvent.put(CollectionEventConstants.EventAttributes.EVENT_NAME,
        CollectionEventConstants.EventAttributes.OFFLINE_STUDENT_COLLECTION_PERF_EVENT);
    createCollectionContext(reports, cpEvent, context);
    JsonObject result = new JsonObject();
    if (views != null) {
      result.put(CollectionEventConstants.EventAttributes.VIEWS, views);
    }
    if (score != null) {
      result.put(CollectionEventConstants.EventAttributes.SCORE, score);
    }
    if (timespent != null) {
      result.put(CollectionEventConstants.EventAttributes.TIMESPENT, timespent);
    }
    if (maxScore != null) {
      result.put(CollectionEventConstants.EventAttributes.MAX_SCORE, maxScore);
    }
    if (reaction != null) {
      result.put(CollectionEventConstants.EventAttributes.REACTION, reaction);
    }
    if (isGraded != null) {
      result.put(CollectionEventConstants.EventAttributes.IS_GRADED, isGraded);
    }
    result.put(CollectionEventConstants.EventAttributes.STATUS, true);
    cpEvent.put(CollectionEventConstants.EventAttributes.RESULT, result);
    return cpEvent;
  }

}
