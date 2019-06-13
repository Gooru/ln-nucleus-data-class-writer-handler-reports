package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.validators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ValidationUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ValidationUtils.class);
  private ValidationUtils() {
    throw new AssertionError();
  }

  public static boolean validateScoreAndMaxScore(Double score, Double maxScore) {
    return !(score == null || maxScore == null || (score.compareTo(0.00) < 0)
        || (maxScore.compareTo(0.00) < 0) || (maxScore.compareTo(0.00) == 0)
        || (score.compareTo(maxScore) > 0));
  }
  
  public static boolean validateMaxScore(Double maxScore) {
    return !(maxScore == null || (maxScore.compareTo(0.00) < 0) || (maxScore.compareTo(0.00) == 0));
  }

}
