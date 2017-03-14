package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities;

import java.sql.SQLException;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @author insightsTeam
 *
 */

@Table("taxonomy_report")
public class AJEntityTaxonomyReporting extends Model{
  
    private static final Logger LOGGER = LoggerFactory.getLogger(AJEntityTaxonomyReporting.class);
      
    public static final String ID = "id";
    public static final String SEQUENCE_ID = "sequence_id";
    public static final String SESSION_ID = "session_id";
    //actorId is userId or gooruuid
    public static final String GOORUUID = "actor_id";    
    public static final String TENANT_ID = "tenant_id"; 
    
    public static final String SUBJECT_ID = "subject_id";
    public static final String COURSE_ID = "course_id";
    public static final String DOMAIN_ID = "domain_id";
    public static final String STANDARD_ID = "standard_id";
    //public static final String MICRO_STANDARD_ID = "micro_standard_id";
    public static final String LEARNING_TARGET_ID = "learning_target_id";

    public static final String COLLECTION_ID = "collection_id";
    public static final String RESOURCE_ID = "resource_id";

    public static final String CONTENT_TYPE = "content_type";
    public static final String RESOURCE_TYPE = "resource_type";
    public static final String QUESTION_TYPE = "question_type";
    public static final String ANSWER_OBJECT = "answer_object";

    public static final String VIEWS = "views";
    public static final String REACTION = "reaction";
    public static final String TIMESPENT = "time_spent";
    public static final String SCORE = "score";
    //enum (correct / incorrect / skipped / unevaluated)​
    public static final String RESOURCE_ATTEMPT_STATUS = "resourceAttemptStatus";    
    
    public static final String SELECT_TAXONOMY_REPORT_MAX_SEQUENCE_ID =
            "SELECT max(sequence_id) FROM taxonomy_report";
   
    
    public static final String RESOURCE_ATTEMPT_STATUS_TYPE = "attempt_status";    
    public static final String PGTYPE_TEXT = "text";
    
    public static final String INSERT_TAXONOMY_REPORT = "INSERT INTO taxonomy_report (sequence_id, session_id, actor_id, subject_id, course_id,domain_id, standard_id, learning_target_id, display_code, collection_id, resource_id, resource_type, question_type, answer_object, resource_attempt_status, views, reaction, score, time_spent) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    
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
