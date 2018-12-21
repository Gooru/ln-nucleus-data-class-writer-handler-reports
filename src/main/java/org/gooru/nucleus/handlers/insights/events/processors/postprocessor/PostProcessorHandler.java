package org.gooru.nucleus.handlers.insights.events.processors.postprocessor;

import io.vertx.core.json.JsonObject;
import org.gooru.nucleus.handlers.insights.events.processors.postprocessor.diagnosticassessment.DiagnosticAssessmentPlayedHandler;

/**
 * @author ashish.
 */

public interface PostProcessorHandler {

  void handle();

  static PostProcessorHandler buildDiagnosticAssessmentPlayedHandler(JsonObject request) {
    return new DiagnosticAssessmentPlayedHandler(request);
  }
}
