package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher;

import org.gooru.nucleus.handlers.insights.events.constants.NotificationConstants;
import org.gooru.nucleus.handlers.insights.events.processors.MessageDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.RubricGradingHandler;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityRubricGrading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

/**
 * Created by mukul@gooru
 */

public class RubricGradingEventDispatcher {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(RubricGradingHandler.class);
	public static final String TOPIC_NOTIFICATIONS = "notifications";
	private AJEntityRubricGrading rubricGrading;
	private Long pathId;
	private String pathType;
	private String contextCollectionId;
	private String contextCollectionType;
	
	public RubricGradingEventDispatcher (AJEntityRubricGrading rubricGrading, String pathType, Long pathId, 
			String contextCollectionId, String contextCollectionType) {
		this.pathType = pathType;
		this.pathId = pathId;
		this.rubricGrading = rubricGrading;
		this.contextCollectionId = contextCollectionId;
		this.contextCollectionType = contextCollectionType;
	}
    public void sendGradingCompleteTeacherEventtoNotifications() {
	    JsonObject notificationEvent = createGradingCompleteTeacherNotificationEvent();
    	
	    try {
	      LOGGER.debug("Teacher Grading Notification Event for Teacher : {} ", notificationEvent);
	      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_NOTIFICATIONS, notificationEvent);
	      LOGGER.info("Successfully dispatched Teacher Grading Notification Event..");
	    } catch (Exception e) {
	      LOGGER.error("Error while dispatching Teacher Grading Notification Event ", e);
	    }
	  }
    
    public void sendGradingCompleteStudentEventtoNotifications() {
	    JsonObject notificationEvent = createGradingCompleteStudentNotificationEvent();
    	
	    try {
	      LOGGER.debug("Teacher Grading Notification Event for Student : {} ", notificationEvent);
	      MessageDispatcher.getInstance().sendEvent2Kafka(TOPIC_NOTIFICATIONS, notificationEvent);
	      LOGGER.info("Successfully dispatched Teacher Grading Notification Event..");
	    } catch (Exception e) {
	      LOGGER.error("Error while dispatching Teacher Grading Notification Event ", e);
	    }
	  }
    
    private JsonObject createGradingCompleteTeacherNotificationEvent() {
	    JsonObject selfReportEvent = new JsonObject();		    	    
	    selfReportEvent.put(NotificationConstants.NOTIFICATION_TYPE, NotificationConstants.TEACHER_GRADING_COMPLETE);
	    selfReportEvent.put(NotificationConstants.USER_ID, rubricGrading.get(AJEntityRubricGrading.STUDENT_ID));
	    selfReportEvent.put(NotificationConstants.CLASS_ID, rubricGrading.get(AJEntityRubricGrading.CLASS_ID));
	    selfReportEvent.put(NotificationConstants.COURSE_ID, rubricGrading.get(AJEntityRubricGrading.COURSE_ID));
	    selfReportEvent.put(NotificationConstants.UNIT_ID, rubricGrading.get(AJEntityRubricGrading.UNIT_ID));
	    selfReportEvent.put(NotificationConstants.LESSON_ID, rubricGrading.get(AJEntityRubricGrading.LESSON_ID));		    
	    selfReportEvent.put(NotificationConstants.COLLECTION_ID, rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID));
	    
	    //TODO: update the CURRENT_ITEM_ID and CURRENT_ITEM_TYPE with contextCollectionId and contextCollectionType
	    //Once these attributes are available (currently they are not)
	    selfReportEvent.put(NotificationConstants.CURRENT_ITEM_ID, rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID));
	    selfReportEvent.put(NotificationConstants.CURRENT_ITEM_TYPE, rubricGrading.get(AJEntityRubricGrading.COLLECTION_TYPE));		    
	    
	    selfReportEvent.put(NotificationConstants.PATH_ID, pathId);
	    selfReportEvent.put(NotificationConstants.PATH_TYPE, pathType);

	    selfReportEvent.put(NotificationConstants.ACTION, NotificationConstants.INITIATE);
	    
	    return selfReportEvent;
	}  
    
    private JsonObject createGradingCompleteStudentNotificationEvent() {
	    JsonObject selfReportEvent = new JsonObject();		    	    
	    selfReportEvent.put(NotificationConstants.NOTIFICATION_TYPE, NotificationConstants.STUDENT_GRADABLE_SUBMISSION);
	    selfReportEvent.put(NotificationConstants.USER_ID, rubricGrading.get(AJEntityRubricGrading.STUDENT_ID));
	    selfReportEvent.put(NotificationConstants.CLASS_ID, rubricGrading.get(AJEntityRubricGrading.CLASS_ID));
	    selfReportEvent.put(NotificationConstants.COURSE_ID, rubricGrading.get(AJEntityRubricGrading.COURSE_ID));
	    selfReportEvent.put(NotificationConstants.UNIT_ID, rubricGrading.get(AJEntityRubricGrading.UNIT_ID));
	    selfReportEvent.put(NotificationConstants.LESSON_ID, rubricGrading.get(AJEntityRubricGrading.LESSON_ID));		    
	    selfReportEvent.put(NotificationConstants.COLLECTION_ID, rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID));	    

	    //TODO: update the CURRENT_ITEM_ID and CURRENT_ITEM_TYPE with contextCollectionId and contextCollectionType
	    //Once these attributes are available (currently they are not)
	    selfReportEvent.put(NotificationConstants.CURRENT_ITEM_ID, rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID));
	    selfReportEvent.put(NotificationConstants.CURRENT_ITEM_TYPE, rubricGrading.get(AJEntityRubricGrading.COLLECTION_TYPE));	    
	    
	    selfReportEvent.put(NotificationConstants.PATH_ID, pathId);
	    selfReportEvent.put(NotificationConstants.PATH_TYPE, pathType);
	    selfReportEvent.put(NotificationConstants.ACTION, NotificationConstants.COMPLETE);
	    
	    return selfReportEvent;
	}  

}
