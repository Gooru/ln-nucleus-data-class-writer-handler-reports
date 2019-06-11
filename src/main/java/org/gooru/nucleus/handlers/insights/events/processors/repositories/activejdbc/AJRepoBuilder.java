package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc;

import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.RDAProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.grading.GradingContext;
import org.gooru.nucleus.handlers.insights.events.processors.oa.OAContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.BaseReportingRepo;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.GradingRepo;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.OARepo;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.ReportDataAggregateRepo;

public class AJRepoBuilder {

  public static BaseReportingRepo buildBaseReportingRepo(ProcessorContext context) {
    return new AJBaseReportingRepo(context);
  }

  public static ReportDataAggregateRepo buildCollectionReportDataAggregateRepo(
      RDAProcessorContext context) {
    return new AJReportDataAggregateRepo(context);
  }
  
  public static OARepo buildOARepo(OAContext context) {
    return new AJOARepo(context);
  }
  
  public static GradingRepo buildGradingRepo(GradingContext context) {
    return new AJGradingRepo(context);
  }
}
