package org.gooru.nucleus.handlers.insights.events.processors.postprocessor.diagnosticassessment;

import io.vertx.core.json.JsonObject;
import org.gooru.nucleus.handlers.insights.events.processors.postprocessor.PostProcessorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ashish.
 */

public class DiagnosticAssessmentPlayedHandler implements PostProcessorHandler {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(DiagnosticAssessmentPlayedHandler.class);
  private final JsonObject request;
  private DiagnosticAssessmentPlayedCommand command;

  public DiagnosticAssessmentPlayedHandler(JsonObject request) {
    this.request = request;
  }

  @Override
  public void handle() {
    // Create command and validate params
    LOGGER.debug("Creating command");
    command = DiagnosticAssessmentPlayedCommand.build(request);
    LOGGER.debug("Command created: '{}'", command);
    // Enrich with gutcodes/subject code which are going to be completed
    LOGGER.debug("Enrich with gut codes and subject code");
    DiagnosticAssessmentCommandEnricher.build(command).enrich();
    LOGGER.debug("Command after enrichment: '{}'", command);
    // Update LPCS* tables with specified values
    LOGGER.debug("Will update LP tables");
    new LearnerProfileDao().updateLPCS(command);
  }
}
