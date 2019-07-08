package org.gooru.nucleus.handlers.insights.events.processors.grading;

import org.gooru.nucleus.handlers.insights.events.processors.repositories.RepoBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author mukul@gooru
 */
public class SelfGrader extends Grader {
  private static final Logger LOGGER = LoggerFactory.getLogger(SelfGrader.class);

 
  @Override
  protected MessageResponse processDCAOfflineActivity() {
    MessageResponse response = null;
    try {
      response = RepoBuilder.buildGradingRepo(context).processDCAOASelfGrades();
    } catch (Throwable t) {
      LOGGER.error("Exception while processing Student Self Grades", t.getMessage());
      response = MessageResponseFactory.createInternalErrorResponse(t.getMessage());
    }
    return response;
  }

}
