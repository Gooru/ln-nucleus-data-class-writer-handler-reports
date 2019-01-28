package org.gooru.nucleus.handlers.insights.events.processors.postprocessor.diagnosticassessment;

import java.util.UUID;
import org.gooru.nucleus.handlers.insights.events.app.components.DataSourceRegistry;
import org.javalite.activejdbc.DB;
import org.javalite.activejdbc.LazyList;
import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.DbName;
import org.javalite.activejdbc.annotations.Table;

/**
 * @author ashish.
 */

@DbName("coreDb")
@Table("diagnostic_assessment")
public class AJEntityDiagnosticAssessment extends Model {

  private static final String GRADE_ID = "grade_id";
  private static final String GUT_SUBJECT = "gut_subject";
  private static final String FW_CODE = "fw_code";
  private static final String ASSESSMENT_ID = "assessment_id";
  private static final String LANGUAGE_ID = "language_id";


  public Long fetchGradeId() {
    return this.getLong(GRADE_ID);
  }

  public String fetchGutSubject() {
    return this.getString(GUT_SUBJECT);
  }

  public String fetchFwCode() {
    return this.getString(FW_CODE);
  }

  public UUID fetchAssessmentId() {
    return UUID.fromString(this.getString(ASSESSMENT_ID));
  }

  public Long fetchLanguageId() {
    return this.getLong(LANGUAGE_ID);
  }

  public static AJEntityDiagnosticAssessment fetchDiagAsmtByAssessmentId(UUID asmtId) {
    DB coreDb = new DB("coreDb");
    try {
      coreDb.open(DataSourceRegistry.getInstance().getCoreDataSource());
      coreDb.openTransaction();
      LazyList<AJEntityDiagnosticAssessment> results = AJEntityDiagnosticAssessment
          .find("assessment_id = ?::uuid", asmtId.toString());
      coreDb.commitTransaction();
      if (results != null) {
        return results.get(0);
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
