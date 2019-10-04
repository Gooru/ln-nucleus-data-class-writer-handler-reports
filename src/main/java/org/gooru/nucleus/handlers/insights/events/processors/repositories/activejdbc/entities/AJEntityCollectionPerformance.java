package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities;

import java.sql.SQLException;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author renuka@gooru
 */
@Table("collection_performance")
public class AJEntityCollectionPerformance extends Model {

  private static final Logger LOGGER = LoggerFactory.getLogger(AJEntityCollectionPerformance.class);

  public static final String DATE_IN_TIME_ZONE = "date_in_time_zone";
  public static final String PGTYPE_DATE = "date";

  public void setDateinTZ(String date) {
    setPGObject(DATE_IN_TIME_ZONE, PGTYPE_DATE, date);
  }

  public static final String CHECK_DUPLICATE_COLLECTION_EVENT = "SELECT id, views, timespent, score FROM collection_performance WHERE actor_id = ? AND session_id = ? AND collection_id = ? ";

  public static final String UPDATE_COLLECTION_METRICS =
      "UPDATE collection_performance SET views = ?, timespent= ?, score = ?, max_score = ?,  reaction = ?, is_graded = ?, status = ?, updated_at = ? WHERE id = ?";

  public static final String UPDATE_COLLECTION_TIMESPENT = "UPDATE collection_performance SET timespent = ?, updated_at = ? WHERE id = ?";

  public static final String UPDATE_ASSESSMENT_SCORE = "UPDATE collection_performance SET score = ?, max_score = ?, is_graded = ? WHERE id = ?";

  public static final String UPDATE_CA_COLLECTION_TIMESPENT = "UPDATE collection_performance SET timespent = ? WHERE id = ?";

  public Boolean isPathTypeValidForContentSource(String contentSource, String pathType) {
    switch (contentSource) {
      case EventConstants.COURSEMAP:
        return EventConstants.CM_PATH_TYPES.matcher(pathType).matches();
      case EventConstants.DCA:
        return EventConstants.CA_PATH_TYPES.matcher(pathType).matches();
      case EventConstants.COMPETENCY_MASTERY:
        return EventConstants.PF_PATH_TYPES.matcher(pathType).matches();
      default:
        return false;
    }
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

  public AJEntityCollectionPerformance() {
    // Turning off create_at and updated_at columns are getting updated by
    // activeJDBC.
    this.manageTime(false);
  }
}
