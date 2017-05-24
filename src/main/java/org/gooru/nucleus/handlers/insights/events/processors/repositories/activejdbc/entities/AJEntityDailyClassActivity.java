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

@Table("daily_class_activity")
public class AJEntityDailyClassActivity extends Model {

  	private static final Logger LOGGER = LoggerFactory.getLogger(AJEntityDailyClassActivity.class);
  	public static final String ID = "id";
  	public static final String EVENTNAME = "event_name";
  	
  	public static final String EVENTTYPE = "event_type";
  	//actor_id is userId or gooruuid
  	public static final String GOORUUID = "actor_id";    
  	public static final String TENANT_ID = "tenant_id";
      
  	public static final Object CLASS_GOORU_OID = "class_id";
  	public static final String COURSE_GOORU_OID = "course_id";
  	public static final String UNIT_GOORU_OID = "unit_id";
  	public static final String LESSON_GOORU_OID = "lesson_id";
    public static final String COLLECTION_OID = "collection_id";

    public static final String QUESTION_COUNT = "question_count";
    public static final String SESSION_ID = "session_id";
    public static final String COLLECTION_TYPE = "collection_type";
    public static final String RESOURCE_TYPE = "resource_type";
    public static final String QUESTION_TYPE = "question_type";
    public static final String ANSWER_OBJECT = "answer_object";
    public static final String RESOURCE_ID = "resource_id";
    
    public static final String TIMESPENT = "time_spent";
    public static final String VIEWS = "views";
    public static final String REACTION = "reaction";
    //(correct / incorrect / skipped / unevaluated)​
    public static final String RESOURCE_ATTEMPT_STATUS = "resource_attempt_status";    
    public static final String SCORE = "score";
    //********************************************
    
    public static final String APP_ID = "app_id";
    public static final String PARTNER_ID = "partner_id";
    public static final String COLLECTION_SUB_TYPE = "collection_sub_type";
    public static final String MAX_SCORE = "max_score";
    public static final String PATH_ID = "path_id";
    
    public static final String CREATE_TIMESTAMP = "created_at";
    public static final String UPDATE_TIMESTAMP = "updated_at";   
    
    //public static final String SELECT_BASEREPORT_MAX_SEQUENCE_ID =
      //      "SELECT max(sequence_id) FROM base_reports";

    public static final String GET_COLLECTION_SCORE = 
        "SELECT SUM(score) as score from daily_class_activity "
        + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? AND session_id = ? AND actor_id = ?";
    
    public static final String GET_QUESTION_COUNT = 
        "SELECT question_count from daily_class_activity "
        + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? "
        + "AND session_id = ? AND actor_id = ? AND question_count IS NOT NULL";
    
    public static final String COMPUTE_ASSESSMENT_SCORE = "SELECT SUM(questionData.question_score) AS score, SUM(questionData.resource_timeSpent) as time_spent "
              +"FROM  (SELECT DISTINCT ON (resource_id)  score AS question_score , time_spent as resource_timespent, session_id FROM daily_class_activity "
              +"WHERE event_name = 'collection.resource.play' AND event_type = 'stop' AND session_id = ? "
              +"AND resource_type = 'question' ORDER BY resource_id, updated_at desc) questionData GROUP BY session_id";    
    
    public static final String FIND_COLLECTION_EVENT = "SELECT id, views, time_spent, score, reaction, resource_attempt_status, answer_object "
    		+ "FROM daily_class_activity "
            + "WHERE session_id = ? AND collection_id = ? AND event_type = ? AND event_name = ? ";
    
    public static final String UPDATE_COLLECTION_EVENT = "UPDATE daily_class_activity SET views = ?, time_spent= ?, score = ?, updated_at = ?, "
            + "reaction = ? WHERE id = ?";
    
    public static final String UPDATE_RESOURCE_EVENT = "UPDATE daily_class_activity SET views = ?, time_spent= ?, score = ?, updated_at = ?, "
            + "reaction = ?, resource_attempt_status = ?, answer_object = ? WHERE id = ?";

    
    public static final String FIND_RESOURCE_EVENT = "SELECT id, views, time_spent, score, reaction, resource_attempt_status, answer_object"
    		+ " FROM daily_class_activity WHERE collection_id = ? AND session_id = ? AND resource_id = ? AND event_type = ?";
    
    public static final String RESOURCE_ATTEMPT_STATUS_TYPE = "attempt_status";    
    public static final String PGTYPE_TEXT = "text";
    public static final String PGTYPE_NUMERIC = "numeric";
    public static final String PGTYPE_INT = "smallint";
    
    public void setResourceAttemptStatus(String answerStatus) {
        setPGObject(RESOURCE_ATTEMPT_STATUS, RESOURCE_ATTEMPT_STATUS_TYPE, answerStatus);
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
    public AJEntityDailyClassActivity() {
      // Turning off create_at and updated_at columns are getting updated by
      // activeJDBC.    	
      this.manageTime(false);
    }

}
