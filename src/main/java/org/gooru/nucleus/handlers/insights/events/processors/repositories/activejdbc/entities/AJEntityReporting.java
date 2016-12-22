package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;


/**
 * created by mukul@gooru
 *
 */

@Table("BaseReports")
public class AJEntityReporting extends Model {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AJEntityReporting.class);
    	
	public static final String ID = "id";
	public static final String SEQUENCE_ID = "sequence_id";
	public static final String EVENTNAME = "eventName";
	
	public static final String EVENTTYPE = "eventType";
	//actorId is userId or gooruuid
	public static final String GOORUUID = "actorId";    
    
	public static final Object CLASS_GOORU_OID = "classId";
	public static final String COURSE_GOORU_OID = "courseId";
	public static final String UNIT_GOORU_OID = "unitId";
	public static final String LESSON_GOORU_OID = "lessonId";
    public static final String COLLECTION_OID = "collectionId";

    public static final String QUESTION_COUNT = "question_count";
    public static final String SESSION_ID = "sessionId";
    public static final String COLLECTION_TYPE = "collectionType";
    public static final String RESOURCE_TYPE = "resourceType";
    public static final String QUESTION_TYPE = "questionType";
    public static final String ANSWER_OBJECT = "answerObject";
    public static final String RESOURCE_ID = "resourceId";
    
    public static final String RESOURCE_VIEWS = "resourceViews";
    public static final String COLLECTION_VIEWS = "collectionViews";
    public static final String RESOURCE_TIMESPENT = "resourceTimeSpent";
    public static final String COLLECTION_TIMESPENT = "collectionTimeSpent";
    public static final String VIEWS = "views";
    public static final String REACTION = "reaction";
    //enum (correct / incorrect / skipped / unevaluated)​
    public static final String RESOURCE_ATTEMPT_STATUS = "resourceAttemptStatus";    
    public static final String SCORE = "score";
    public static final String CREATE_TIMESTAMP = "createTimestamp";
    public static final String UPDATE_TIMESTAMP = "updateTimestamp";   
    
    public static final String SELECT_BASEREPORT_MAX_SEQUENCE_ID =
            "SELECT max(sequence_id) FROM BaseReports";
   
    
    public static final String RESOURCE_ATTEMPT_STATUS_TYPE = "attempt_status";    
    public static final String PGTYPE_TEXT = "text";
    
    public void setResourceAttemptStatus(String answerStatus) {
        setPGObject(RESOURCE_ATTEMPT_STATUS, RESOURCE_ATTEMPT_STATUS_TYPE, answerStatus);
    }
        
    public void setAnswerObject(String answerArray){
    	setPGObject(ANSWER_OBJECT, PGTYPE_TEXT, answerArray);
    }
    
    private void setPGObject(String field, String type, String value) {
        PGobject pgObject = new PGobject();
        pgObject.setType(type);
        try {
            pgObject.setValue(value);
            this.set(field, pgObject);
        } catch (SQLException e) {
            LOGGER.error("Not able to set value for field: {}, type: {}, value: {}", field, type, value);
            this.errors().put(field, value);
        }
    }

}
