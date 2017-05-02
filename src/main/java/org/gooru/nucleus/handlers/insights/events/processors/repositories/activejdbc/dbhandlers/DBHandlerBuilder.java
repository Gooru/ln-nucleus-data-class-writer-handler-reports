package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;

/**
 * Created by mukul@gooru
 */
  public final class DBHandlerBuilder {
  
    private DBHandlerBuilder() {
      throw new AssertionError();
    }
  
    public static DBHandler buildProcessEventHandler(ProcessorContext context) {
      return new ProcessEventHandler(context);
    }
  
    public static DBHandler buildCompetencyReportsHandler(ProcessorContext context) {
      return new ProcessCompetencyReportHandler(context);
    }
  
    public static DBHandler buildDailyClassActivityEventHandler(ProcessorContext context) {
      return new DailyClassActivityEventHandler(context);
    }
  
    public static DBHandler buildDCACompetencyHandler(ProcessorContext context) {
      return new DCACompetencyHandler(context);
    }
  
    public static DBHandler buildUserTaxonomySubjectHandler(ProcessorContext context) {
      return new UserTaxonomySubjectHandler(context);
    }
}
