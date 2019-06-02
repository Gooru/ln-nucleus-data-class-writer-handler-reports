package org.gooru.nucleus.handlers.insights.events.processors.repositories;

import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;

/**
 * @author mukul@gooru
 */
public interface GradingRepo {

  MessageResponse processDCAOASelfGrades();
  
  MessageResponse processDCAOATeacherGrades();

}
