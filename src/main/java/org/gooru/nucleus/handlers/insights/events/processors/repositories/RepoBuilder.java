package org.gooru.nucleus.handlers.insights.events.processors.repositories;

import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.RDAProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.grading.GradingContext;
import org.gooru.nucleus.handlers.insights.events.processors.oa.OAContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.AJRepoBuilder;

public class RepoBuilder {

  public static BaseReportingRepo buildBaseReportingRepo(ProcessorContext context) {
    return AJRepoBuilder.buildBaseReportingRepo(context);
  }

  public static ReportDataAggregateRepo buildReportDataAggregateRepo(RDAProcessorContext context) {
    return AJRepoBuilder.buildCollectionReportDataAggregateRepo(context);
  }
  
  public static OARepo buildOARepo(OAContext context) {
    return AJRepoBuilder.buildOARepo(context);
  }
  
  public static GradingRepo buildGradingRepo(GradingContext context) {
    return AJRepoBuilder.buildGradingRepo(context);
  }
}


