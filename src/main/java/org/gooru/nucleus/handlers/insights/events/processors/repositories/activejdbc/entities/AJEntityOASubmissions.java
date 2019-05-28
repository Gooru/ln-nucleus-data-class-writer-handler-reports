package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mukul@gooru
 */
@Table("offline_activity_submissions")
public class AJEntityOASubmissions extends Model {

  private static final Logger LOGGER = LoggerFactory.getLogger(AJEntityOASubmissions.class);
  
  public static final String ID = "id";  
  public static final String OA_ID = "oa_id";
  public static final String OA_TYPE = "oa_type";
  public static final String OA_SUBTYPE = "oa_subtype";
  public static final String TASK_ID = "task_id";
  public static final String SUBMISSION_ID = "submission_id";
  public static final String SUBMISSION_TYPE = "submission_type";
  public static final String SUBMISSION_SUBTYPE = "submission_subtype";
  public static final String SUBMISSION = "submission";
  public static final String STUDENT_ID = "student_id";
  public static final String CLASS_ID = "class_id";
  public static final String CREATED_AT = "created_at";
  public static final String UPDATED_AT = "updated_at";
  
  public static final String GET_STUDENTS_FOR_OA =
      "SELECT distinct(student_id) from offline_activity_submissions where class_id = ? AND oa_id = ?";
    
}
