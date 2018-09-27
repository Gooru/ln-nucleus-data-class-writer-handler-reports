package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities;

import java.sql.SQLException;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author renuka@gooru
 * 
 */
@Table("collection_performance")
public class AjEntityCollectionPerformance extends Model {

    private static final Logger LOGGER = LoggerFactory.getLogger(AjEntityCollectionPerformance.class);

    public static final String DATE_IN_TIME_ZONE = "date_in_time_zone";
    public static final String PGTYPE_DATE = "date";

    public void setDateinTZ(String date) {
        setPGObject(DATE_IN_TIME_ZONE, PGTYPE_DATE, date);
    }

    public static final String CHECK_DUPLICATE_COLLECTION_EVENT = "SELECT id, views, timespent, score FROM collection_performance WHERE actor_id = ? AND session_id = ? AND collection_id = ? ";

    public static final String UPDATE_COLLECTION_METRICS =
        "UPDATE collection_performance SET views = ?, timespent= ?, score = ?, max_score = ?,  reaction = ?, is_graded = ?, updated_at = ? WHERE id = ?";

    public static final String UPDATE_COLLECTION_TIMESPENT = "UPDATE collection_performance SET timespent = ?, updated_at = ? WHERE id = ?";

    public static final String UPDATE_ASSESSMENT_SCORE = "UPDATE collection_performance SET score = ?, max_score = ?, is_graded = ? WHERE session_id = ? AND collection_id = ?";

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

    public AjEntityCollectionPerformance() {
        // Turning off create_at and updated_at columns are getting updated by
        // activeJDBC.
        this.manageTime(false);
    }
}
