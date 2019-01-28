package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;

@Table("course_collection_count")
public class AJEntityCourseCollectionCount extends Model {

  public static final String COURSE_ID = "course_id";
  public static final String UNIT_ID = "unit_id";
  public static final String LESSON_ID = "lesson_id";
  public static final String COLLECTION_FORMAT = "collection_format";
  public static final String COLLECTION_COUNT = "collection_count";
  public static final String ASSESSMENT_COUNT = "assessment_count";
  public static final String EXT_ASSESSMENT_COUNT = "ext_assessment_count";
  public static final String CREATED_AT = "created_at";
  public static final String UPDATED_AT = "updated_at";

  //collection format type :{collection,assessment,external-assessment}
  public static final String ATTR_ASSESSMENT = "assessment";
  public static final String ATTR_EXTERNAL_ASSESSMENT = "assessment-external";
  public static final String ATTR_COLLECTION = "collection";

  public static final String ATTR_COURSE = "course";
  public static final String ATTR_UNIT = "unit";
  public static final String ATTR_LESSON = "lesson";

  public static final String GET_COURSE_ASSESSMENT_COUNT =
      "SELECT SUM(assessment_count) as totalCount FROM course_collection_count WHERE course_id = ? GROUP BY course_id";

  public static final String UPDATE_ALL_COUNT = "UPDATE course_collection_count SET collection_count = ? , assessment_count = ? , ext_assessment_count = ? WHERE course_id = ? AND unit_id = ? AND lesson_id = ?";

  public static final String UPDATE_COLLECTION_COUNT = "UPDATE course_collection_count SET collection_count = (collection_count + ?) WHERE course_id = ? AND unit_id = ? AND lesson_id = ?";

  public static final String UPDATE_ASSESSMENT_COUNT = "UPDATE course_collection_count SET assessment_count = (assessment_count + ?)  WHERE course_id = ? AND unit_id = ? AND lesson_id = ?";

  public static final String UPDATE_EXT_ASSESSMENT_COUNT = "UPDATE course_collection_count SET ext_assessment_count = (ext_assessment_count + ?) WHERE course_id = ? AND unit_id = ? AND lesson_id = ?";

  public static final String INSERT_COURSE_COLLECTION_COUNT = "INSERT INTO course_collection_count (course_id , unit_id , lesson_id , collection_count , assessment_count , ext_assessment_count)VALUES (?,?,?,?,?,?)";

  public static final String DELETE_COURSE_LEVEL = "DELETE FROM course_collection_count WHERE course_id = ?";

  public static final String DELETE_UNIT_LEVEL = "DELETE FROM course_collection_count WHERE course_id = ? AND unit_id = ?";

  public static final String DELETE_LESSON_LEVEL = "DELETE FROM course_collection_count WHERE course_id = ? AND unit_id = ? AND lesson_id = ?";

  public static final String SELECT_ROW_COUNT = "SELECT COUNT(1) AS rowCount FROM course_collection_count WHERE course_id = ? AND unit_id = ? AND lesson_id = ?";

  public AJEntityCourseCollectionCount() {
    // Turning off create_at and updated_at columns are getting updated by
    // activeJDBC.
    this.manageTime(false);
  }
}
