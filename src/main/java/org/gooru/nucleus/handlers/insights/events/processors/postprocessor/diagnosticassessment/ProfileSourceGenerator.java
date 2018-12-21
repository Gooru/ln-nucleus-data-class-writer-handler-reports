package org.gooru.nucleus.handlers.insights.events.processors.postprocessor.diagnosticassessment;

import java.util.Objects;
import java.util.UUID;

/**
 * @author ashish.
 */

public final class ProfileSourceGenerator {

  private static final String DIAGNOSTIC = "diagnostic";

  private ProfileSourceGenerator() {
    throw new AssertionError();
  }

  public static String generateProfileSource(UUID assessmentId) {
    return DIAGNOSTIC + ":" + Objects.toString(assessmentId);
  }
}
