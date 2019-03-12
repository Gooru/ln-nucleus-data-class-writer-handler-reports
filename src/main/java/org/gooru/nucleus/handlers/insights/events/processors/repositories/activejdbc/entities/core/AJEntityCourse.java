package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.core;

import java.util.UUID;
import org.gooru.nucleus.handlers.insights.events.app.components.DataSourceRegistry;
import org.javalite.activejdbc.DB;
import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.DbName;
import org.javalite.activejdbc.annotations.Table;

/**
 * @author renuka.
 */

@DbName("coreDb")
@Table("course")
public class AJEntityCourse extends Model {

  private static final String VERSION = "version";
  private static final String PREMIUM = "premium";
  private static final String IS_DELETED =  "is_deleted";
  
  public static Boolean isPremium(AJEntityCourse course) {
    return (course.getString(VERSION) != null && course.getString(VERSION).equals(PREMIUM) && !course.getBoolean(IS_DELETED));
  }

  public static AJEntityCourse fetchCourse(UUID courseId) {
    DB coreDb = new DB("coreDb");
    try {
      coreDb.open(DataSourceRegistry.getInstance().getCoreDataSource());
      coreDb.openTransaction();
      AJEntityCourse results = AJEntityCourse.findById(courseId);
      coreDb.commitTransaction();
      if (results != null) {
        return results;
      }
      return null;
    } catch (Throwable throwable) {
      coreDb.rollbackTransaction();
      throw throwable;
    } finally {
      coreDb.close();
    }
  }


}
