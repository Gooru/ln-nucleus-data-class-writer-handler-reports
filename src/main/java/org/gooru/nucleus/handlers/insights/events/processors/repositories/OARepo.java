package org.gooru.nucleus.handlers.insights.events.processors.repositories;

import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;

/**
 * @author mukul@gooru
 */
public interface OARepo {

  MessageResponse processOAEvent();

  MessageResponse processOASelfGrades();
  
  MessageResponse storeSubmissionDetails();

  MessageResponse processOACompletionEvent();

}
