package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.grading;

import org.gooru.nucleus.handlers.insights.events.processors.grading.GradingContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.DBHandler;


/**
 * @author mukul@gooru
 */
public class GradingDBHandlerBuilder {

  public static DBHandler buildDCAOASelfGradingHandler(GradingContext context) {
    return new DCAOASelfGradingHandler(context);
  }
  
  public static DBHandler buildDCAOATeacherGradingHandler(GradingContext context) {
    return new DCAOATeacherGradingHandler(context);
  }

}
