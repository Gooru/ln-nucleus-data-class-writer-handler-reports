package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;

/**
 * @author daniel
 */
@Table("user_tax_subject")
public class AJEntityUserTaxonomySubject extends Model {

  public static final String ID = "id";

  public static final String TAX_SUBJECT_ID = "tax_subject_id";

  public static final String COURSE_ID = "course_id";

  public static final String ACTOR_ID = "actor_id";

  public static final String UPDATED_AT = "updated_at";

  public static final String CLASS_ID = "class_id";

  public static final String SELECT_SUBJECT_ID_BY_COURSE = "SELECT tax_subject_id FROM content WHERE id = ?";

  public AJEntityUserTaxonomySubject() {
    // Turning off create_at and updated_at columns are getting updated by
    // activeJDBC.
    this.manageTime(false);
  }
}
