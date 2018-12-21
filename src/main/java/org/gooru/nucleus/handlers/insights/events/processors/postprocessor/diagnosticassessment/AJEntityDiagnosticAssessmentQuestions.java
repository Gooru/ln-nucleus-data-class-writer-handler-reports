package org.gooru.nucleus.handlers.insights.events.processors.postprocessor.diagnosticassessment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.gooru.nucleus.handlers.insights.events.app.components.DataSourceRegistry;
import org.javalite.activejdbc.DB;
import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.DbName;
import org.javalite.activejdbc.annotations.Table;

/**
 * @author ashish.
 */
@DbName("coreDb")
@Table("diagnostic_assessment_questions")
public class AJEntityDiagnosticAssessmentQuestions extends Model {

  public static List<String> fetchGutCodesForSpecifiedQuestions(List<String> questionIds,
      UUID assessmentId) {
    DB coreDb = new DB("coreDb");
    try {
      coreDb.open(DataSourceRegistry.getInstance().getCoreDataSource());
      coreDb.openTransaction();
      List gutCodes = coreDb.firstColumn(
          "select gut_code from diagnostic_assessment_questions where diagnostic_assessment_id = ?::uuid "
              + " and question_id = ANY(?::uuid[])", assessmentId.toString(),
          Utils.convertListToPostgresArrayStringRepresentation(questionIds));
      List<String> result = new ArrayList<>(gutCodes.size());
      for (Object gutCode : gutCodes) {
        result.add(String.valueOf(gutCode));
      }
      coreDb.commitTransaction();
      return result;
    } catch (Throwable throwable) {
      coreDb.rollbackTransaction();
      throw throwable;
    } finally {
      coreDb.close();
    }
  }

}
