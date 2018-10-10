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
public class StudentSelfReportingEventDispatcher {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(StudentSelfReportingEventDispatcher.class);
	public static final String TOPIC_NOTIFICATIONS = "notifications";
	private AJEntityReporting baseReports;
	
	public StudentSelfReportingEventDispatcher (AJEntityReporting baseReports) {	
		this.baseReports = baseReports;
	}


    public void sendSelfReportEventtoNotifications() {

    	try {
    		JsonObject notificationEvent = createSelfReportNotificationEvent();
    		LOGGER.debug("Student Self Reporting Notification Event : {} ", notificationEvent);
    		MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_NOTIFICATIONS, notificationEvent);
    		LOGGER.info("Successfully dispatched Student Self Reporting Notification Event..");
    	} catch (Exception e) {
    		LOGGER.error("Error while dispatching Student Self Reporting Notification Event ", e);
    	}
	  }
    
    private JsonObject createSelfReportNotificationEvent() {
	    JsonObject selfReportEvent = new JsonObject();		    	    
	    selfReportEvent.put(NotificationConstants.NOTIFICATION_TYPE, NotificationConstants.STUDENT_SELF_REPORT);
	    selfReportEvent.put(NotificationConstants.USER_ID, baseReports.get(AJEntityReporting.GOORUUID));
	    selfReportEvent.put(NotificationConstants.CLASS_ID, baseReports.get(AJEntityReporting.CLASS_GOORU_OID));
	    selfReportEvent.put(NotificationConstants.COURSE_ID, baseReports.get(AJEntityReporting.COURSE_GOORU_OID));
	    selfReportEvent.put(NotificationConstants.UNIT_ID, baseReports.get(AJEntityReporting.UNIT_GOORU_OID));
	    selfReportEvent.put(NotificationConstants.LESSON_ID, baseReports.get(AJEntityReporting.LESSON_GOORU_OID));		    
	    selfReportEvent.put(NotificationConstants.COLLECTION_ID, baseReports.get(AJEntityReporting.COLLECTION_OID));
	    
	    //TODO: update the CURRENT_ITEM_ID and CURRENT_ITEM_TYPE with contextCollectionId and contextCollectionType
	    //Once these attributes are available (currently they are not)
	    selfReportEvent.put(NotificationConstants.CURRENT_ITEM_ID, baseReports.get(AJEntityReporting.COLLECTION_OID));
	    selfReportEvent.put(NotificationConstants.CURRENT_ITEM_TYPE, baseReports.get(AJEntityReporting.COLLECTION_TYPE));		    
	    selfReportEvent.put(NotificationConstants.PATH_ID, baseReports.get(AJEntityReporting.PATH_ID));
	    selfReportEvent.put(NotificationConstants.PATH_TYPE, baseReports.get(AJEntityReporting.PATH_TYPE));
	    selfReportEvent.put(NotificationConstants.ACTION, NotificationConstants.INITIATE);
	    
	    return selfReportEvent;
	}  	    
}
