package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.converters.ConverterRegistry;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.converters.FieldConverter;
import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * created by mukul@gooru
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
  public static final String PATH_TYPE = "path_type";

  public static final String EVENT_ID = "event_id";
  public static final String TIME_ZONE = "time_zone";
  public static final String DATE_IN_TIME_ZONE = "date_in_time_zone";
  public static final String IS_GRADED = "is_graded";
  public static final String CONTENT_SOURCE = "content_source";
  public static final String CONTEXT_COLLECTION_ID = "context_collection_id";
  public static final String CONTEXT_COLLECTION_TYPE = "context_collection_type";
  public static final String ADDITIONAL_CONTEXT = "additional_context";

  public static final String CREATE_TIMESTAMP = "created_at";
  public static final String UPDATE_TIMESTAMP = "updated_at";
  public static final String GRADING_TYPE = "grading_type";

  public static final String GET_COLLECTION_SCORE =
      "SELECT SUM(score) as score from daily_class_activity "
          + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? AND session_id = ? AND actor_id = ?";

  public static final String GET_QUESTION_COUNT =
      "SELECT question_count from daily_class_activity "
          + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? "
          + "AND session_id = ? AND actor_id = ? AND question_count IS NOT NULL";

  public static final String COMPUTE_ASSESSMENT_SCORE =
      "SELECT SUM(questionData.question_score) AS score, SUM(questionData.max_score) "
          + "AS max_score, SUM(questionData.resource_timeSpent) AS time_spent FROM  (SELECT DISTINCT ON (resource_id) score AS "
          + "question_score, max_score AS max_score, time_spent AS resource_timespent, session_id FROM daily_class_activity "
          + "WHERE collection_id = ? AND session_id = ? AND event_name = 'collection.resource.play' AND event_type = 'stop' "
          + "AND resource_type = 'question' ORDER BY resource_id, updated_at desc) questionData GROUP BY session_id";

  public static final String FIND_COLLECTION_EVENT =
      "SELECT id, views, time_spent, score, reaction, resource_attempt_status, answer_object "
          + "FROM daily_class_activity "
          + "WHERE actor_id = ? AND session_id = ? AND collection_id = ? AND event_type = ? AND event_name = ? ";

//    public static final String UPDATE_COLLECTION_EVENT = "UPDATE daily_class_activity SET views = ?, time_spent= ?, score = ?, updated_at = ?, "
//            + "reaction = ? WHERE id = ?";

  public static final String UPDATE_COLLECTION_EVENT =
      "UPDATE daily_class_activity SET views = ?, time_spent= ?, score = ?, max_score = ?, updated_at = ?, "
          + "reaction = ? WHERE id = ?";

//    public static final String UPDATE_RESOURCE_EVENT = "UPDATE daily_class_activity SET views = ?, time_spent= ?, score = ?, updated_at = ?, "
//            + "reaction = ?, resource_attempt_status = ?, answer_object = ? WHERE id = ?";

  public static final String UPDATE_RESOURCE_EVENT =
      "UPDATE daily_class_activity SET views = ?, time_spent= ?, score = ?, updated_at = ?, "
          + "reaction = ?, resource_attempt_status = ?, answer_object = ? WHERE id = ?";


  public static final String FIND_RESOURCE_EVENT =
      "SELECT id, views, time_spent, score, reaction, resource_attempt_status, answer_object"
          + " FROM daily_class_activity WHERE actor_id = ? AND collection_id = ? AND session_id = ? AND resource_id = ? AND event_type = ?";

  public static final String SELECT_DCA_REPORT_ID = "SELECT id FROM daily_class_activity WHERE collection_id = ? AND session_id = ? AND resource_id = ? AND event_type = ? ";

  //Student Grading of External Assessment
  public static final String CHECK_IF_EXT_ASSESSMENT_SELF_GRADED =
      "SELECT id, views, time_spent, score, reaction FROM daily_class_activity "
          + "WHERE actor_id = ? AND class_id = ? AND collection_id = ? AND session_id = ? AND event_name = ? AND event_type = ? ";

  public static final String UPDATE_SELF_GRADED_EXT_ASSESSMENT =
      "UPDATE daily_class_activity SET views = ?, time_spent = ?, score = ?, max_score = ?, updated_at = ?, "
          + "time_zone = ?, date_in_time_zone = ? WHERE id = ?";

  public static final String UPDATE_ASSESSMENT_SCORE_U =
      "UPDATE daily_class_activity SET score = ?, max_score = ? WHERE actor_id = ? AND class_id = ? "
          + "AND session_id =  ? AND collection_id = ? AND event_name = 'collection.play' AND event_type = 'stop'";

  public static final String UPDATE_QUESTION_SCORE_U =
      "UPDATE daily_class_activity SET score = ?, max_score = ?, is_graded = ? "
          + "WHERE actor_id = ? AND class_id = ? AND session_id = ? AND collection_id = ? AND resource_id = ? AND event_name = 'collection.resource.play' "
          + "AND event_type = 'stop'";

  public static final String COMPUTE_ASSESSMENT_SCORE_POST_GRADING_U =
      "SELECT SUM(questionData.question_score) AS score, "
          + "SUM(questionData.max_score) AS max_score FROM  "
          + "(SELECT DISTINCT ON (resource_id)  score AS question_score, max_score, "
          + "session_id FROM daily_class_activity WHERE actor_id = ? AND class_id = ? AND collection_id = ? AND session_id = ? AND "
          + "event_name = 'collection.resource.play' AND event_type = 'stop' AND resource_type = 'question' "
          + "ORDER BY resource_id, updated_at desc) questionData GROUP BY session_id";

  public static final String IS_COLLECTION_GRADED = "SELECT is_graded FROM daily_class_activity "
      + "WHERE actor_id = ? AND session_id = ? AND collection_id = ?  AND event_name = ? AND event_type = ? AND is_graded = ?";

  public static final String COMPUTE_TIMESPENT =
      "SELECT SUM(tsData.resource_timespent) as time_spent "
          + "FROM  (SELECT DISTINCT ON (resource_id) time_spent as resource_timespent, session_id FROM daily_class_activity "
          + "WHERE collection_id = ? AND session_id = ? AND event_name = 'collection.resource.play' AND event_type = 'stop' "
          + "ORDER BY resource_id, updated_at desc) tsData GROUP BY session_id";
  
  //Rubric Grading of FRQ Questions at DCA *****************************************************************************************************
  //Case when the same Assessments spills-over to the next date than the activity assigned date, then the resource date_in_time_zone
  //will be different than the Assessment completed (collection.play) date_in_time_zone. (ideally however this should not happen, 
  //for each date the activity is scheduled, we should have a new session of play)
  //TO TEST: Currently, Students can play Activities scheduled ONLY FOR TODAY. Play of previous date's activities is disabled. (if this 
  //is not the case then date_in_time_zone param in the below queries may need to revisited)
  public static final String UPDATE_QUESTION_SCORE = "UPDATE daily_class_activity SET score = ?, max_score = ?, is_graded = ? WHERE "
      + "actor_id = ? AND collection_id =? AND session_id = ? AND resource_id = ?";
  public static final String UPDATE_ASSESSMENT_SCORE =
      "UPDATE daily_class_activity SET score = ?, max_score = ? WHERE actor_id = ? AND collection_id =? AND session_id = ? "
          + "AND event_name = 'collection.play' AND event_type = 'stop'";
  public static final String COMPUTE_ASSESSMENT_SCORE_POST_GRADING =
      "SELECT SUM(questionData.question_score) AS score, "
          + "SUM(questionData.max_score) AS max_score FROM  "
          + "(SELECT DISTINCT ON (resource_id)  score AS question_score, max_score, "
          + "session_id FROM daily_class_activity WHERE actor_id = ? AND collection_id = ? AND session_id = ? AND "
          + "event_name = 'collection.resource.play' AND event_type = 'stop' AND resource_type = 'question' "
          + "ORDER BY resource_id, updated_at desc) questionData GROUP BY session_id";

  // **************************************************************************************************************************
  // PERF UPDATE HANDLER
  public static final String UPDATE_RESOURCE_TS_SCORE =
      "UPDATE daily_class_activity SET time_spent= ?, max_score = ?, score = ? WHERE id = ?";

  public static final String UPDATE_RESOURCE_TS =
      "UPDATE daily_class_activity SET time_spent= ? WHERE id = ?";

  public static final String UPDATE_OVERALL_COLLECTION_PERF =
      "UPDATE daily_class_activity SET time_spent= ?, max_score = ?, score = ? WHERE id = ?";

  public static final String UPDATE_OVERALL_COLLECTION_TS =
      "UPDATE daily_class_activity SET time_spent= ? WHERE id = ?";

  public static final String RESOURCE_ATTEMPT_STATUS_TYPE = "attempt_status";
  public static final String PGTYPE_TEXT = "text";
  public static final String PGTYPE_NUMERIC = "numeric";
  public static final String PGTYPE_INT = "smallint";
  public static final String PGTYPE_DATE = "date";

  public void setResourceAttemptStatus(String answerStatus) {
    setPGObject(RESOURCE_ATTEMPT_STATUS, RESOURCE_ATTEMPT_STATUS_TYPE, answerStatus);
  }

  public void setDateinTZ(String date) {
    setPGObject(DATE_IN_TIME_ZONE, PGTYPE_DATE, date);
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

  //*********************************************************************************************************************************************

  private static final Map<String, FieldConverter> converterRegistry;

  static {
    converterRegistry = initializeConverters();
  }

  private static Map<String, FieldConverter> initializeConverters() {
    Map<String, FieldConverter> converterMap = new HashMap<>();
    converterMap
        .put(ANSWER_OBJECT, (fieldValue -> FieldConverter.convertJsonToTextArray(fieldValue)));
    return Collections.unmodifiableMap(converterMap);
  }

  public static ConverterRegistry getConverterRegistry() {
    return new DCAConverterRegistry();
  }

  private static class DCAConverterRegistry implements ConverterRegistry {

    @Override
    public FieldConverter lookupConverter(String fieldName) {
      return converterRegistry.get(fieldName);
    }
  }

  //**********************************************************************************************************************************************

}
