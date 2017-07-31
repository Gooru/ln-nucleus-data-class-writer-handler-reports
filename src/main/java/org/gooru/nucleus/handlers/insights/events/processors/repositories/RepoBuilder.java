package org.gooru.nucleus.handlers.insights.events.processors.repositories;

import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.AJRepoBuilder;

public class RepoBuilder {
 
    public static BaseReportingRepo buildBaseReportingRepo(ProcessorContext context) {
        return AJRepoBuilder.buildBaseReportingRepo(context);
    }

    public RubricGradingRepo buildRubricGradingRepo(ProcessorContext context) {
      return AJRepoBuilder.buildRubricsGradingRepo(context);
  }
}


