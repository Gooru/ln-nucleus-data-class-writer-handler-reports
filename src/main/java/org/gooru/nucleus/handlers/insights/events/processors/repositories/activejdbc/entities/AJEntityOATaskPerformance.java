package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** 
 * @author mukul@gooru
 */
@Table("offline_activity_task_perf")
public class AJEntityOATaskPerformance extends Model {

  private static final Logger LOGGER = LoggerFactory.getLogger(AJEntityOASubmissions.class);
  
  public static final String ID = "id";
  public static final String OA_ID = "oa_id";
  public static final String TASK_ID = "task_id";
  public static final String STUDENT_ID = "student_id";
  public static final String STUDENT_SCORE = "student_score";
  public static final String MAX_SCORE = "max_score";
  public static final String TIMESPENT = "time_spent";  
  public static final String GRADER = "grader";
  public static final String CREATED_AT = "created_at";
  public static final String UPDATED_AT = "updated_at";  
  
  public static final String GET_OA_PERFORMANCE =
      "select student_id, SUM(student_score) AS student_score, SUM(max_score) AS max_score, SUM(time_spent) AS time_spent "
      + "from offline_activity_task_perf where oa_id = ? and student_id = ANY(?::uuid[]) GROUP BY student_id";

}

