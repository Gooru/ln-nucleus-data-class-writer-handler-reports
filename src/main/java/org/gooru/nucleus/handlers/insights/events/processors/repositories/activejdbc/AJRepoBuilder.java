package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc;

import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.BaseReportingRepo;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.RubricGradingRepo;

public class AJRepoBuilder {
    
    public static BaseReportingRepo buildBaseReportingRepo(ProcessorContext context) {
        return new AJBaseReportingRepo(context);
    }
    
    public static RubricGradingRepo buildRubricsGradingRepo(ProcessorContext context) {
      return new AJRubricGradingRepo(context);
  }
}
