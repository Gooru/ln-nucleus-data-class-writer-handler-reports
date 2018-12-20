package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author daniel
 */

@Table("competency_report")
public class AJEntityCompetencyReport extends Model {

  private static final Logger LOGGER = LoggerFactory.getLogger(AJEntityCompetencyReport.class);
  public static final String ID = "id";
  public static final String SESSION_ID = "session_id";
  public static final String ACTOR_ID = "actor_id";
  public static final Object CLASS_ID = "class_id";
  public static final String COURSE_ID = "course_id";
  public static final String UNIT_ID = "unit_id";
  public static final String LESSON_ID = "lesson_id";
  public static final Object TAX_SUBJECT_ID = "tax_subject_id";
  public static final String TAX_COURSE_ID = "tax_course_id";
  public static final String TAX_DOMAIN_ID = "tax_domain_id";
  public static final String TAX_STANDARD_ID = "tax_standard_id";
  public static final String TAX_MICRO_STANDARD_ID = "tax_micro_standard_id";
  public static final String TENANT_ID = "tenant_id";
  public static final String DISPLAY_CODE = "display_code";
  public static final String COLLECTION_ID = "collection_id";
  public static final String RESOURCE_ID = "resource_id";
  public static final String RESOURCE_TYPE = "resource_type";
  public static final String EVENT_TYPE = "event_type";
  public static final String COLLECTION_TYPE = "collection_type";
  public static final String BASE_REPORT_ID = "base_report_id";
  public static final String CREATED_AT = "created_at";
  public static final String UPDATED_AT = "updated_at";

  public static final String SELECT_ROWS_BY_SESSION_ID_AND_RESOURCE = "session_id = ? AND resource_id = ? AND event_type = ? ";

  public AJEntityCompetencyReport() {
    // Turning off create_at and updated_at columns are getting updated by
    // activeJDBC.
    this.manageTime(false);
  }
}
