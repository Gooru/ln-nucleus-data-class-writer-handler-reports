package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher;

import org.gooru.nucleus.handlers.insights.events.constants.NotificationConstants;
import org.gooru.nucleus.handlers.insights.events.processors.MessageDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

/**
 * Created by mukul@gooru
 *   
 */
public class TeacherScoreOverideEventDispatcher {
	
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TeacherScoreOverideEventDispatcher.class);
	public static final String TOPIC_NOTIFICATIONS = "notifications";
	private AJEntityReporting baseReports;
	
	public TeacherScoreOverideEventDispatcher (AJEntityReporting baseReports) {
		this.baseReports = baseReports;
	}

	public void sendTeacherScoreUpdateEventtoNotifications() {
		JsonObject notificationEvent = createTeacherScoreUpdateNotificationEvent();

		try {
			LOGGER.debug("Teacher Score Overide Notification Event : {} ", notificationEvent);
			MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_NOTIFICATIONS, notificationEvent);
			LOGGER.info("Successfully dispatched Teacher Score Overide Notification Event..");
		} catch (Exception e) {
			LOGGER.error("Error while dispatching Teacher Score Overide Notification Event ", e);
		}
	}

	private JsonObject createTeacherScoreUpdateNotificationEvent() {		    	    
		JsonObject teacherScoreUpdateEvent = new JsonObject();		    	    
		teacherScoreUpdateEvent.put(NotificationConstants.NOTIFICATION_TYPE, NotificationConstants.TEACHER_SCORE_OVERRIDE);
		teacherScoreUpdateEvent.put(NotificationConstants.USER_ID, baseReports.get(AJEntityReporting.GOORUUID));
		teacherScoreUpdateEvent.put(NotificationConstants.CLASS_ID, baseReports.get(AJEntityReporting.CLASS_GOORU_OID));
		teacherScoreUpdateEvent.put(NotificationConstants.COURSE_ID, baseReports.get(AJEntityReporting.COURSE_GOORU_OID));
		teacherScoreUpdateEvent.put(NotificationConstants.UNIT_ID, baseReports.get(AJEntityReporting.UNIT_GOORU_OID));
		teacherScoreUpdateEvent.put(NotificationConstants.LESSON_ID, baseReports.get(AJEntityReporting.LESSON_GOORU_OID));		    
		teacherScoreUpdateEvent.put(NotificationConstants.COLLECTION_ID, baseReports.get(AJEntityReporting.COLLECTION_OID));
		
	    //TODO: update the CURRENT_ITEM_ID and CURRENT_ITEM_TYPE with contextCollectionId and contextCollectionType
	    //Once these attributes are available (currently they are not)
		teacherScoreUpdateEvent.put(NotificationConstants.CURRENT_ITEM_ID, baseReports.get(AJEntityReporting.COLLECTION_OID));
		teacherScoreUpdateEvent.put(NotificationConstants.CURRENT_ITEM_TYPE, baseReports.get(AJEntityReporting.COLLECTION_TYPE));		    
		teacherScoreUpdateEvent.put(NotificationConstants.PATH_ID, baseReports.get(AJEntityReporting.PATH_ID));
		teacherScoreUpdateEvent.put(NotificationConstants.PATH_TYPE, baseReports.get(AJEntityReporting.PATH_TYPE));
		teacherScoreUpdateEvent.put(NotificationConstants.ACTION, NotificationConstants.INITIATE);

		return teacherScoreUpdateEvent;
	}  
}
