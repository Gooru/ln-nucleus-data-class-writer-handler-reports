package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities;

import java.sql.SQLException;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
    //Mukul - latest changes 25/01/17
    public static final String COLLECTION_SCORE = "collectionScore";
    //********************************************
    public static final String CREATE_TIMESTAMP = "createTimestamp";
    public static final String UPDATE_TIMESTAMP = "updateTimestamp";   
    
    public static final String SELECT_BASEREPORT_MAX_SEQUENCE_ID =
            "SELECT max(sequence_id) FROM BaseReports";

    public static final String GET_COLLECTION_SCORE = 
        "SELECT SUM(score) as score from basereports "
        + "WHERE classId = ? AND courseId = ? AND unitId = ? AND lessonId = ? AND collectionId = ? AND sessionId = ? AND actorId = ?";
    
    public static final String GET_QUESTION_COUNT = 
        "SELECT question_count from basereports "
        + "WHERE classId = ? AND courseId = ? AND unitId = ? AND lessonId = ? AND collectionId = ? "
        + "AND sessionId = ? AND actorId = ? AND question_count IS NOT NULL";
        
    public static final String CHECK_DUP_RESOURCE_EVENT = 
        "SELECT resourceTimeSpent, resourceViews from basereports "
        + "WHERE classId = ? AND courseId = ? AND unitId = ? AND lessonId = ? AND "
        + "collectionId = ? AND resourceId = ? AND sessionId = ? AND eventType = ? "
        + "AND collectionType = ? AND actorId = ?";
    
    public static final String RESOURCE_ATTEMPT_STATUS_TYPE = "attempt_status";    
    public static final String PGTYPE_TEXT = "text";
    public static final String PGTYPE_NUMERIC = "numeric";
    public static final String PGTYPE_INT = "smallint";
    
    public void setResourceAttemptStatus(String answerStatus) {
        setPGObject(RESOURCE_ATTEMPT_STATUS, RESOURCE_ATTEMPT_STATUS_TYPE, answerStatus);
    }
    
    public void setCollectionScore(String collScore){
      setPGObject(COLLECTION_SCORE, PGTYPE_NUMERIC, collScore);
    }
    
    //TODO: Need to revisit this. PGObject doesn't like any integer (int, smallint, bigInt) datatype passed to it.
    //Throws an exception - org.javalite.activejdbc.DBException: org.postgresql.util.PSQLException: Unknown type smallint
    //However, with Numeric proper data is stored in the PG for Datatype bigint.
    public void setResourceViews(String resViews){
      setPGObject(RESOURCE_VIEWS, PGTYPE_NUMERIC, resViews);
    }
    
    public void setResourceTimeSpent(String resTS){
      setPGObject(RESOURCE_TIMESPENT, PGTYPE_NUMERIC, resTS);
    }
    
    public void setAnswerObject(String answerArray){
    	setPGObject(ANSWER_OBJECT, PGTYPE_TEXT, answerArray);
    }
    /*************************** DELETE Queries For ReComputations Purpose *************************/
    
    public static final String DELETE_BASEREPORT_BY_COURSE = "DELETE FROM BaseReports WHERE classId = ? AND courseId = ?";
   
    public static final String DELETE_BASEREPORT_BY_UNIT = "DELETE FROM BaseReports WHERE classId = ? AND unitId = ?";
    
    public static final String DELETE_BASEREPORT_BY_LESSON = "DELETE FROM BaseReports WHERE classId = ? AND lessonId = ?";
    
    public static final String DELETE_BASEREPORT_BY_COLLECTION = "DELETE FROM BaseReports WHERE classId = ? AND collectionId = ?";
    
    /***************************/
    
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
