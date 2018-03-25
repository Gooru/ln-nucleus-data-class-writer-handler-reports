package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.converters.ConverterRegistry;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.converters.FieldConverter;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.validators.FieldValidator;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.validators.ValidatorRegistry;
import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * created by mukul@gooru
 * modified by daniel
 */

@Table("base_reports")
public class AJEntityReporting extends Model {
  	private static final Logger LOGGER = LoggerFactory.getLogger(AJEntityReporting.class);
  	public static final String ID = "id";
  	public static final String SEQUENCE_ID = "sequence_id";
  	public static final String EVENTNAME = "event_name";
  	
  	public static final String EVENTTYPE = "event_type";
  	//actor_id is userId or gooruuid
  	public static final String GOORUUID = "actor_id";    
  	public static final String TENANT_ID = "tenant_id";
      
  	public static final String CLASS_GOORU_OID = "class_id";
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
    
    public static final String EVENT_ID = "event_id";
    public static final String TIME_ZONE = "time_zone";
    public static final String DATE_IN_TIME_ZONE = "date_in_time_zone";
    public static final String IS_GRADED = "is_graded";
    public static final String CONTENT_SOURCE = "content_source";

    public static final String CREATE_TIMESTAMP = "created_at";
    public static final String UPDATE_TIMESTAMP = "updated_at";   
    
    public static final String SELECT_BASEREPORT_MAX_SEQUENCE_ID =
            "SELECT max(sequence_id) FROM base_reports";

    public static final String GET_COLLECTION_SCORE = 
        "SELECT SUM(score) as score from base_reports "
        + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? AND session_id = ? AND actor_id = ?";
    
    public static final String GET_QUESTION_COUNT = 
        "SELECT question_count from base_reports "
        + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? "
        + "AND session_id = ? AND actor_id = ? AND question_count IS NOT NULL";
    
    public static final String GET_QUESTION_COUNT_SESS = 
            "SELECT question_count from base_reports "
            + "WHERE collection_id = ? "
            + "AND session_id = ? AND event_name = 'collection.play' AND question_count IS NOT NULL";
    
    public static final String COMPUTE_ASSESSMENT_SCORE_POST_GRADING = "SELECT SUM(questionData.question_score) AS score, "
    		+ "SUM(questionData.max_score) AS max_score FROM  "
    		+ "(SELECT DISTINCT ON (resource_id)  score AS question_score, max_score, "
    		+ "session_id FROM base_reports WHERE collection_id = ? AND session_id = ? AND "
    		+ "event_name = 'collection.resource.play' AND event_type = 'stop' AND resource_type = 'question' "
    		+ "ORDER BY resource_id, updated_at desc) questionData GROUP BY session_id";
    
    public static final String COMPUTE_ASSESSMENT_SCORE = "SELECT SUM(questionData.question_score) AS score, "
    		+ "SUM(questionData.max_score) as max_score, SUM(questionData.resource_timeSpent) as time_spent "
            + "FROM  (SELECT DISTINCT ON (resource_id) score AS question_score, max_score AS max_score, "
            + "time_spent as resource_timespent, session_id FROM base_reports "
            + "WHERE collection_id = ? AND session_id = ? AND event_name = 'collection.resource.play' AND event_type = 'stop' "
            + "AND resource_type = 'question' ORDER BY resource_id, updated_at desc) questionData GROUP BY session_id";
    
    public static final String FIND_COLLECTION_EVENT = "SELECT id,views,time_spent,score,reaction,resource_attempt_status,answer_object FROM base_reports "
            + "WHERE session_id = ? AND collection_id = ? AND event_type = ? AND event_name = ? ";
    
    public static final String UPDATE_COLLECTION_EVENT = "UPDATE base_reports SET views = ?, time_spent= ?, score = ?, max_score = ?, updated_at = ?, "
            + "reaction = ? WHERE id = ?";
    
    public static final String UPDATE_RESOURCE_EVENT = "UPDATE base_reports SET views = ?, time_spent= ?, score = ?, updated_at = ?, "
            + "reaction = ?, resource_attempt_status = ?, answer_object = ? WHERE id = ?";

    
    public static final String FIND_RESOURCE_EVENT = "SELECT id,views,time_spent,score,reaction,resource_attempt_status,answer_object FROM base_reports "
            + "WHERE collection_id = ? AND session_id = ? AND resource_id = ? AND event_type = ?";
    
    public static final String FIND_SESSION_ID = "SELECT session_id "
            + "FROM base_reports WHERE class_id = ? AND course_id = ? AND unit_id = ? "
            + "AND lesson_id = ? AND collection_id = ? AND resource_id = ? "
            + "ORDER BY updated_at desc ,session_id LIMIT 1";
    
    public static final String FIND_COLLECTION_TYPE = "SELECT collection_type FROM base_reports WHERE class_id = ? AND course_id = ? "
    		+ "AND unit_id = ? AND lesson_id = ? AND collection_id = ? AND event_name = 'collection.play' LIMIT 1";
    
    //TODO: Update these queries related to Rubrics Scoring.
    public static final String UPDATE_QUESTION_SCORE = "UPDATE base_reports SET score = ?, max_score = ?, is_graded = ? WHERE session_id = ? AND resource_id = ?";    
    public static final String UPDATE_ASSESSMENT_SCORE = "UPDATE base_reports SET score = ?, max_score = ? WHERE collection_id = ? AND session_id =  ? "
    		+ "AND event_name = 'collection.play' AND event_type = 'stop'";
    
    public static final String UPDATE_QUESTION_SCORE_U = "UPDATE base_reports SET score = ?, is_graded = ?, resource_attempt_status = ? "
    		+ "WHERE actor_id = ? AND class_id = ? AND session_id = ? AND collection_id = ? AND resource_id = ? AND event_name = 'collection.resource.play' "
    		+ "AND event_type = 'stop'";
    
    public static final String UPDATE_ASSESSMENT_SCORE_U = "UPDATE base_reports SET score = ?, max_score = ? WHERE actor_id = ? AND class_id = ? "
    		+ "AND session_id =  ? AND collection_id = ? AND event_name = 'collection.play' AND event_type = 'stop'";
    
    public static final String COMPUTE_ASSESSMENT_SCORE_POST_GRADING_U = "SELECT SUM(questionData.question_score) AS score, "
    		+ "SUM(questionData.max_score) AS max_score FROM  "
    		+ "(SELECT DISTINCT ON (resource_id)  score AS question_score, max_score, "
    		+ "session_id FROM base_reports WHERE actor_id = ? AND class_id = ? AND collection_id = ? AND session_id = ? AND "
    		+ "event_name = 'collection.resource.play' AND event_type = 'stop' AND resource_type = 'question' "
    		+ "ORDER BY resource_id, updated_at desc) questionData GROUP BY session_id";

    

    public static final String RESOURCE_ATTEMPT_STATUS_TYPE = "attempt_status";    
    public static final String PGTYPE_TEXT = "text";
    public static final String PGTYPE_NUMERIC = "numeric";
    public static final String PGTYPE_INT = "smallint";
    public static final String PGTYPE_DATE = "date";
    
    public void setDateinTZ(String date) {
        setPGObject(DATE_IN_TIME_ZONE, PGTYPE_DATE, date);
    }
    
    /*************************** DELETE Queries For ReComputations Purpose *************************/
    
    public static final String DELETE_BASEREPORT_BY_COURSE = "DELETE FROM base_reports WHERE class_id = ? AND course_id = ?";
   
    public static final String DELETE_BASEREPORT_BY_UNIT = "DELETE FROM base_reports WHERE class_id = ? AND unit_id = ?";
    
    public static final String DELETE_BASEREPORT_BY_LESSON = "DELETE FROM base_reports WHERE class_id = ? AND lesson_id = ?";
    
    public static final String DELETE_BASEREPORT_BY_COLLECTION = "DELETE FROM base_reports WHERE class_id = ? AND collection_id = ?";
    
    /***************************/
    
    public static final String SELECT_BASE_REPORT_ID = "SELECT id FROM base_reports WHERE collection_id = ? AND session_id = ? AND resource_id = ? AND event_type = ? ";
    
    //*********************************************************************************************************************************************
    
    private static final Map<String, FieldValidator> validatorRegistry;
    private static final Map<String, FieldConverter> converterRegistry;

    static {
        validatorRegistry = initializeValidators();
        converterRegistry = initializeConverters();
    }

    private static Map<String, FieldConverter> initializeConverters() {
        Map<String, FieldConverter> converterMap = new HashMap<>();
        converterMap.put(ANSWER_OBJECT, (fieldValue -> FieldConverter.convertJsonToTextArray(fieldValue)));        
        return Collections.unmodifiableMap(converterMap);
    }

    private static Map<String, FieldValidator> initializeValidators() {
        Map<String, FieldValidator> validatorMap = new HashMap<>();
        validatorMap.put(ANSWER_OBJECT, FieldValidator::validateJsonIfPresent);
        return Collections.unmodifiableMap(validatorMap);
    }

    public static ValidatorRegistry getValidatorRegistry() {
        return new BaseReportsValidationRegistry();
    }

    public static ConverterRegistry getConverterRegistry() {
        return new BaseReportsConverterRegistry();
    }

    private static class BaseReportsValidationRegistry implements ValidatorRegistry {
        @Override
        public FieldValidator lookupValidator(String fieldName) {
            return validatorRegistry.get(fieldName);
        }
    }

    private static class BaseReportsConverterRegistry implements ConverterRegistry {
        @Override
        public FieldConverter lookupConverter(String fieldName) {
            return converterRegistry.get(fieldName);
        }
    }
    
    //**********************************************************************************************************************************************
    
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
    public AJEntityReporting() {
      // Turning off create_at and updated_at columns are getting updated by
      // activeJDBC.
      this.manageTime(false);
    }
}
