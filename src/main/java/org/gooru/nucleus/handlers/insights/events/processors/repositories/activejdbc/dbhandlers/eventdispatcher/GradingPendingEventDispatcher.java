package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher;

import io.vertx.core.json.JsonObject;
import org.gooru.nucleus.handlers.insights.events.constants.GEPConstants;
import org.gooru.nucleus.handlers.insights.events.constants.NotificationConstants;
import org.gooru.nucleus.handlers.insights.events.processors.MessageDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.javalite.activejdbc.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GradingPendingEventDispatcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(GradingPendingEventDispatcher.class);
  public static final String TOPIC_NOTIFICATIONS = "notifications";
  private AJEntityReporting baseReports;
  private AJEntityDailyClassActivity dcaReports;
  private String additionalContext;

  public GradingPendingEventDispatcher(AJEntityReporting baseReports, String additionalContext) {
    this.baseReports = baseReports;
    this.additionalContext = additionalContext;
  }

  public GradingPendingEventDispatcher(AJEntityDailyClassActivity dcaReports, String additionalContext) {
    this.dcaReports = dcaReports;
    this.additionalContext = additionalContext;
  }
  
  public void sendGradingPendingEventtoNotifications() {
    try {
      JsonObject notificationEvent = createGradingPendingNotificationEvent(baseReports);
      LOGGER.debug("Student Grading Pending Notification Event : {} ", notificationEvent.toString());
      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_NOTIFICATIONS, notificationEvent);
      LOGGER.info("Successfully dispatched Student Grading Pending Event..");
    } catch (Exception e) {
      LOGGER.error("Error while dispatching Student Grading Pending Event ", e);
    }
  }

  public void sendDCAGradingPendingEventtoNotifications() {
    try {
      JsonObject notificationEvent = createDCAGradingPendingNotificationEvent(dcaReports);
      LOGGER.debug("Student DCA Grading Pending Notification Event : {} ", notificationEvent.toString());
      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_NOTIFICATIONS, notificationEvent);      
      LOGGER.info("Successfully dispatched Student DCA Grading Pending Event..");
    } catch (Exception e) {
      LOGGER.error("Error while dispatching Student DCA Grading Pending Event ", e);
    }
  }

  private JsonObject createGradingPendingNotificationEvent(Model reports) {
    JsonObject pendingGradingEvent = new JsonObject();
    pendingGradingEvent.put(NotificationConstants.NOTIFICATION_TYPE,
        NotificationConstants.STUDENT_GRADABLE_SUBMISSION);
    pendingGradingEvent.put(NotificationConstants.USER_ID, reports.get(AJEntityReporting.GOORUUID));
    pendingGradingEvent
        .put(NotificationConstants.CLASS_ID, reports.get(AJEntityReporting.CLASS_GOORU_OID));
    pendingGradingEvent
        .put(NotificationConstants.COURSE_ID, reports.get(AJEntityReporting.COURSE_GOORU_OID));
    pendingGradingEvent
        .put(NotificationConstants.UNIT_ID, reports.get(AJEntityReporting.UNIT_GOORU_OID));
    pendingGradingEvent
        .put(NotificationConstants.LESSON_ID, reports.get(AJEntityReporting.LESSON_GOORU_OID));
    pendingGradingEvent
        .put(NotificationConstants.COLLECTION_ID, reports.get(AJEntityReporting.COLLECTION_OID));
    // TODO: update the CURRENT_ITEM_ID and CURRENT_ITEM_TYPE with
    // contextCollectionId and contextCollectionType
    // Once these attributes are available (currently they are not)
    pendingGradingEvent
        .put(NotificationConstants.CURRENT_ITEM_ID, reports.get(AJEntityReporting.COLLECTION_OID));
    pendingGradingEvent.put(NotificationConstants.CURRENT_ITEM_TYPE,
        reports.get(AJEntityReporting.COLLECTION_TYPE));
    pendingGradingEvent.put(NotificationConstants.PATH_ID, reports.get(AJEntityReporting.PATH_ID));
    pendingGradingEvent
        .put(NotificationConstants.PATH_TYPE, reports.get(AJEntityReporting.PATH_TYPE));
    //additionalContext is currently NULL for courseMap
    pendingGradingEvent.putNull(GEPConstants.ADDITIONAL_CONTEXT);
    pendingGradingEvent
    .put(NotificationConstants.CONTENT_SOURCE, reports.get(AJEntityReporting.CONTENT_SOURCE));

    pendingGradingEvent.put(NotificationConstants.ACTION, NotificationConstants.INITIATE);

    return pendingGradingEvent;
  }
  
  private JsonObject createDCAGradingPendingNotificationEvent(Model reports) {
    JsonObject pendingGradingEvent = new JsonObject();
    pendingGradingEvent.put(NotificationConstants.NOTIFICATION_TYPE,
        NotificationConstants.STUDENT_GRADABLE_SUBMISSION);
    pendingGradingEvent.put(NotificationConstants.USER_ID, reports.get(AJEntityDailyClassActivity.GOORUUID));
    pendingGradingEvent
        .put(NotificationConstants.CLASS_ID, reports.get(AJEntityDailyClassActivity.CLASS_GOORU_OID));
    pendingGradingEvent
        .put(NotificationConstants.COURSE_ID, reports.get(AJEntityDailyClassActivity.COURSE_GOORU_OID));
    pendingGradingEvent
        .put(NotificationConstants.UNIT_ID, reports.get(AJEntityDailyClassActivity.UNIT_GOORU_OID));
    pendingGradingEvent
        .put(NotificationConstants.LESSON_ID, reports.get(AJEntityDailyClassActivity.LESSON_GOORU_OID));
    pendingGradingEvent
        .put(NotificationConstants.COLLECTION_ID, reports.get(AJEntityDailyClassActivity.COLLECTION_OID));
    // TODO: update the CURRENT_ITEM_ID and CURRENT_ITEM_TYPE with
    // contextCollectionId and contextCollectionType
    // Once these attributes are available (currently they are not)
    pendingGradingEvent
        .put(NotificationConstants.CURRENT_ITEM_ID, reports.get(AJEntityDailyClassActivity.COLLECTION_OID));
    pendingGradingEvent.put(NotificationConstants.CURRENT_ITEM_TYPE,
        reports.get(AJEntityDailyClassActivity.COLLECTION_TYPE));
    pendingGradingEvent.put(NotificationConstants.PATH_ID, reports.get(AJEntityDailyClassActivity.PATH_ID));
    pendingGradingEvent
        .put(NotificationConstants.PATH_TYPE, reports.get(AJEntityDailyClassActivity.PATH_TYPE));
    pendingGradingEvent.put(GEPConstants.ADDITIONAL_CONTEXT, additionalContext);
    pendingGradingEvent
    .put(NotificationConstants.CONTENT_SOURCE, reports.get(AJEntityReporting.CONTENT_SOURCE));

    pendingGradingEvent.put(NotificationConstants.ACTION, NotificationConstants.INITIATE);

    return pendingGradingEvent;
  }

}
