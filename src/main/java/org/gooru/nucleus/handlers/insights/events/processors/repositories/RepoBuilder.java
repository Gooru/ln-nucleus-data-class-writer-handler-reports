package org.gooru.nucleus.handlers.insights.events.processors.repositories;

import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.RDAProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.AJRepoBuilder;

public class RepoBuilder {

  public static BaseReportingRepo buildBaseReportingRepo(ProcessorContext context) {
    return AJRepoBuilder.buildBaseReportingRepo(context);
  }

  public static ReportDataAggregateRepo buildReportDataAggregateRepo(RDAProcessorContext context) {
    return AJRepoBuilder.buildCollectionReportDataAggregateRepo(context);
  }
}


