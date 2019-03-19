package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher;

import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.GEPConstants;
import org.gooru.nucleus.handlers.insights.events.processors.MessageDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.javalite.activejdbc.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author renuka@gooru
 */
public class GEPEventDispatcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(GEPEventDispatcher.class);
  public static final String TOPIC_GEP = "gep";
  private AJEntityReporting baseReports;
  private AJEntityDailyClassActivity dcaReport;
  private long activityTime;
  private String additionalContext;

  public GEPEventDispatcher(AJEntityDailyClassActivity dcaReport, Long timespent, Double maxScore,
      Double score, long activityTime, String additionalContext) {
    this.dcaReport = dcaReport;
    this.activityTime = activityTime;
    this.additionalContext = additionalContext;
  }

  public GEPEventDispatcher(AJEntityReporting baseReports, Long timespent, Double maxScore,
      Double score, long activityTime, String additionalContext) {
    this.baseReports = baseReports;
    this.activityTime = activityTime;
    this.additionalContext = additionalContext;
  }

  public void sendCPEventFromDCAtoGEP() {
    try {
      JsonObject gepEvent = createCPEvent(dcaReport);
      LOGGER.debug("The Collection GEP Event is : {} ", gepEvent);
      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_GEP, gepEvent);
      LOGGER.info("Successfully dispatched Collection Perf GEP report..");
    } catch (Exception e) {
      LOGGER.error("Error while dispatching Collection Perf GEP Event ", e);
    }
  }

  public void sendCRPEventFromDCAtoGEP() {
    try {
      JsonObject gepEvent = createCRPEvent(dcaReport);
      LOGGER.debug("The Collection Resource GEP Event is : {} ", gepEvent);
      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_GEP, gepEvent);
      LOGGER.info("Successfully dispatched Collection Resource GEP report..");
    } catch (Exception e) {
      LOGGER.error("Error while dispatching Collection Resource GEP Event ", e);
    }
  }

  public void sendCPEventFromBaseReportstoGEP() {
    try {
      JsonObject gepEvent = createCPEvent(baseReports);
      LOGGER.debug("The Collection GEP Event is : {} ", gepEvent);
      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_GEP, gepEvent);
      LOGGER.info("Successfully dispatched Collection Perf GEP report..");
    } catch (Exception e) {
      LOGGER.error("Error while dispatching Collection Perf GEP Event ", e);
    }
  }

  public void sendCRPEventFromBaseReportstoGEP() {
    try {
      JsonObject gepEvent = createCRPEvent(baseReports);
      LOGGER.debug("The Collection Resource GEP Event is : {} ", gepEvent);
      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_GEP, gepEvent);
      LOGGER.info("Successfully dispatched Collection Resource GEP report..");
    } catch (Exception e) {
      LOGGER.error("Error while dispatching Collection Resource GEP Event ", e);
    }
  }

  private JsonObject createCRPEvent(Model report) {
    JsonObject gepCRPEvent = new JsonObject();
    JsonObject context = new JsonObject();

    gepCRPEvent.put(GEPConstants.USER_ID, report.getString(AJEntityDailyClassActivity.GOORUUID));
    gepCRPEvent.put(GEPConstants.ACTIVITY_TIME, this.activityTime);
    gepCRPEvent.put(GEPConstants.EVENT_ID, UUID.randomUUID().toString());
    gepCRPEvent.put(GEPConstants.EVENT_NAME, GEPConstants.RESOURCE_PERF_EVENT);
    gepCRPEvent
        .put(GEPConstants.RESOURCE_ID, report.getString(AJEntityDailyClassActivity.RESOURCE_ID));
    gepCRPEvent.put(GEPConstants.RESOURCE_TYPE,
        report.getString(AJEntityDailyClassActivity.RESOURCE_TYPE));

    context.put(GEPConstants.COLLECTION_ID,
        report.getString(AJEntityDailyClassActivity.COLLECTION_OID));
    context.put(GEPConstants.COLLECTION_TYPE,
        report.getString(AJEntityDailyClassActivity.COLLECTION_TYPE));
    context.put(GEPConstants.CONTEXT_COLLECTION_ID,
        report.getString(AJEntityDailyClassActivity.CONTEXT_COLLECTION_ID));
    context.put(GEPConstants.CONTEXT_COLLECTION_TYPE,
        report.getString(AJEntityDailyClassActivity.CONTEXT_COLLECTION_TYPE));
    context
        .put(GEPConstants.CLASS_ID, report.getString(AJEntityDailyClassActivity.CLASS_GOORU_OID));
    context
        .put(GEPConstants.COURSE_ID, report.getString(AJEntityDailyClassActivity.COURSE_GOORU_OID));
    context.put(GEPConstants.UNIT_ID, report.getString(AJEntityDailyClassActivity.UNIT_GOORU_OID));
    context
        .put(GEPConstants.LESSON_ID, report.getString(AJEntityDailyClassActivity.LESSON_GOORU_OID));
    context.put(GEPConstants.PATH_ID, report.getString(AJEntityDailyClassActivity.PATH_ID));
    context.put(GEPConstants.SESSION_ID, report.getString(AJEntityDailyClassActivity.SESSION_ID));
    context.put(GEPConstants.PARTNER_ID, report.getString(AJEntityDailyClassActivity.PARTNER_ID));
    context.put(GEPConstants.TENANT_ID, report.getString(AJEntityDailyClassActivity.TENANT_ID));
    context.put(GEPConstants.CONTENT_SOURCE, report.get(AJEntityDailyClassActivity.CONTENT_SOURCE));

    context.put(GEPConstants.PATH_TYPE, report.getString(AJEntityDailyClassActivity.PATH_TYPE));
    gepCRPEvent.put(GEPConstants.CONTEXT, context);
    gepCRPEvent.put(GEPConstants.ADDITIONAL_CONTEXT, additionalContext);

    JsonObject result = new JsonObject();
    String resourceType = report.getString(AJEntityDailyClassActivity.RESOURCE_TYPE);
    if (resourceType.equals(EventConstants.QUESTION)) {
      result.put(GEPConstants.SCORE, report.getLong(AJEntityDailyClassActivity.SCORE));
      result.put(GEPConstants.MAX_SCORE, report.getLong(AJEntityDailyClassActivity.MAX_SCORE));
    } else if (resourceType.equals(EventConstants.RESOURCE)) {
      result.putNull(GEPConstants.SCORE);
      result.putNull(GEPConstants.MAX_SCORE);
    }
    result.put(GEPConstants.TIMESPENT, report.getLong(AJEntityDailyClassActivity.TIMESPENT));
    result.put(GEPConstants.ANSWER_STATUS,
        report.getString(AJEntityDailyClassActivity.RESOURCE_ATTEMPT_STATUS));
    gepCRPEvent.put(GEPConstants.RESULT, result);

    return gepCRPEvent;
  }

  private JsonObject createCPEvent(Model report) {
    JsonObject gepCPEvent = new JsonObject();
    JsonObject context = new JsonObject();

    gepCPEvent.put(GEPConstants.USER_ID, report.getString(AJEntityDailyClassActivity.GOORUUID));
    gepCPEvent.put(GEPConstants.ACTIVITY_TIME, this.activityTime);
    gepCPEvent.put(GEPConstants.EVENT_ID, UUID.randomUUID().toString());
    gepCPEvent.put(GEPConstants.EVENT_NAME, GEPConstants.COLLECTION_PERF_EVENT);
    gepCPEvent.put(GEPConstants.COLLECTION_ID,
        report.getString(AJEntityDailyClassActivity.COLLECTION_OID));
    gepCPEvent.put(GEPConstants.COLLECTION_TYPE,
        report.getString(AJEntityDailyClassActivity.COLLECTION_TYPE));
    context.put(GEPConstants.CONTEXT_COLLECTION_ID,
        report.getString(AJEntityDailyClassActivity.CONTEXT_COLLECTION_ID));
    context.put(GEPConstants.CONTEXT_COLLECTION_TYPE,
        report.getString(AJEntityDailyClassActivity.CONTEXT_COLLECTION_TYPE));

    context
        .put(GEPConstants.CLASS_ID, report.getString(AJEntityDailyClassActivity.CLASS_GOORU_OID));
    context
        .put(GEPConstants.COURSE_ID, report.getString(AJEntityDailyClassActivity.COURSE_GOORU_OID));
    context.put(GEPConstants.UNIT_ID, report.getString(AJEntityDailyClassActivity.UNIT_GOORU_OID));
    context
        .put(GEPConstants.LESSON_ID, report.getString(AJEntityDailyClassActivity.LESSON_GOORU_OID));
    context.put(GEPConstants.PATH_ID, report.getString(AJEntityDailyClassActivity.PATH_ID));
    context.put(GEPConstants.SESSION_ID, report.getString(AJEntityDailyClassActivity.SESSION_ID));
    context.put(GEPConstants.QUESTION_COUNT,
        report.getString(AJEntityDailyClassActivity.QUESTION_COUNT));
    context.put(GEPConstants.PARTNER_ID, report.getString(AJEntityDailyClassActivity.PARTNER_ID));
    context.put(GEPConstants.TENANT_ID, report.getString(AJEntityDailyClassActivity.TENANT_ID));
    context.put(GEPConstants.CONTENT_SOURCE, report.get(AJEntityDailyClassActivity.CONTENT_SOURCE));

    context.put(GEPConstants.PATH_TYPE, report.getString(AJEntityDailyClassActivity.PATH_TYPE));
    gepCPEvent.put(GEPConstants.CONTEXT, context);
    gepCPEvent.put(GEPConstants.ADDITIONAL_CONTEXT, additionalContext);

    JsonObject result = new JsonObject();
    String collectionType = report.getString(AJEntityDailyClassActivity.COLLECTION_TYPE);

    if (collectionType.equalsIgnoreCase(EventConstants.EXTERNAL_ASSESSMENT) || collectionType
        .equalsIgnoreCase(EventConstants.EXTERNAL_COLLECTION)) {
      Double ms = report.getDouble(AJEntityDailyClassActivity.MAX_SCORE);
      if (ms != null && ms > 0.0) {
        result.put(GEPConstants.SCORE, report.getDouble(AJEntityDailyClassActivity.SCORE));
      } else {
        result.put(GEPConstants.SCORE, 0.0);
      }
      result.put(GEPConstants.MAX_SCORE, ms);

    } else {
      Double ms = report.getDouble(AJEntityDailyClassActivity.MAX_SCORE);
      Double score = report.getDouble(AJEntityDailyClassActivity.SCORE);
      if (ms != null && ms > 0.0 && score != null) {
        result.put(GEPConstants.SCORE, score);
        result.put(GEPConstants.MAX_SCORE, ms);
      } else {
        // TODO: Should the score be sent as NULL or 0.0
        result.putNull(GEPConstants.SCORE);
        result.put(GEPConstants.MAX_SCORE, 0.0);
      }
    }
    result.put(GEPConstants.TIMESPENT, report.getLong(AJEntityDailyClassActivity.TIMESPENT));

    // This is for future Use. Currently no Reaction is associated the Assessment/Collection
    result.put(GEPConstants.REACTION, 0);
    gepCPEvent.put(GEPConstants.RESULT, result);
    return gepCPEvent;
  }

}
