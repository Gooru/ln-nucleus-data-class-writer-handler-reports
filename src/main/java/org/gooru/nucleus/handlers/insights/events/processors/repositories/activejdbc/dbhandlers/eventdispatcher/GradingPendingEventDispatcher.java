package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher;

import io.vertx.core.json.JsonObject;
import org.gooru.nucleus.handlers.insights.events.constants.NotificationConstants;
import org.gooru.nucleus.handlers.insights.events.processors.MessageDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.javalite.activejdbc.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GradingPendingEventDispatcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(GradingPendingEventDispatcher.class);
  public static final String TOPIC_NOTIFICATIONS = "notifications";
  private AJEntityReporting baseReports;

  public GradingPendingEventDispatcher(AJEntityReporting baseReports) {
    this.baseReports = baseReports;
  }

  public void sendGradingPendingEventtoNotifications() {

    try {
      JsonObject notificationEvent = createGradingPendingNotificationEvent(baseReports);
      LOGGER.debug("Student Grading Pending Notification Event : {} ", notificationEvent);
      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_NOTIFICATIONS, notificationEvent);
      LOGGER.info("Successfully dispatched Student Grading Pending Event..");
    } catch (Exception e) {
      LOGGER.error("Error while dispatching Student Grading Pending Event ", e);
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

    pendingGradingEvent.put(NotificationConstants.ACTION, NotificationConstants.INITIATE);

    return pendingGradingEvent;
  }

}
