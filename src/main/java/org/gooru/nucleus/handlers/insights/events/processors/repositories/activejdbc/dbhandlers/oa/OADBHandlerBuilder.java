package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.oa;

import org.gooru.nucleus.handlers.insights.events.processors.oa.OAContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.DBHandler;


/**
 * @author mukul@gooru
 */
public final class OADBHandlerBuilder {
  
  public static DBHandler buildOAEventHandler(OAContext context) {
    return new OADCAEventHandler(context);
  }

  public static DBHandler buildOASelfGradeHandler(OAContext context) {
    return new OASelfGradingHandler(context);  
  }
  
  public static DBHandler buildOASubmissionsHandler(OAContext context) {
    return new OASubmissionsHandler(context);
  }
  
  public static DBHandler buildOACompletionEventHandler(OAContext context) {
    return new OACompletionEventHandler(context);
  }
}
