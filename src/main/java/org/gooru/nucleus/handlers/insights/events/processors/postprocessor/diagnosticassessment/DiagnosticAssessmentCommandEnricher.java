package org.gooru.nucleus.handlers.insights.events.processors.postprocessor.diagnosticassessment;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ashish.
 */

class DiagnosticAssessmentCommandEnricher {

  private final DiagnosticAssessmentPlayedCommand command;
  private static final Logger LOGGER = LoggerFactory
      .getLogger(DiagnosticAssessmentCommandEnricher.class);

  private DiagnosticAssessmentCommandEnricher(DiagnosticAssessmentPlayedCommand command) {
    this.command = command;
  }

  static DiagnosticAssessmentCommandEnricher build(DiagnosticAssessmentPlayedCommand command) {
    return new DiagnosticAssessmentCommandEnricher(command);
  }

  void enrich() {
    LOGGER.debug("Command enrichment will be done");
    AJEntityDiagnosticAssessment assessment = AJEntityDiagnosticAssessment
        .fetchDiagAsmtByAssessmentId(command.getAssessmentId());
    if (assessment == null) {
      throw new IllegalStateException(
          "Diagnostic assessment not found: " + command.getAssessmentId());
    }

    List<String> gutCodes = AJEntityDiagnosticAssessmentQuestions
        .fetchGutCodesForSpecifiedQuestions(
            Utils.convertListUUIDToListString(command.getQuestions()), command.getAssessmentId());

    command.setGutCodes(gutCodes);
    command.setSubjectCode(assessment.fetchGutSubject());
  }
}
