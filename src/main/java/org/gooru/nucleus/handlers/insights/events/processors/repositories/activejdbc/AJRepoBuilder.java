package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc;

import org.gooru.nucleus.handlers.insights.events.processors.RDAProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.BaseReportingRepo;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.ReportDataAggregateRepo;

public class AJRepoBuilder {
    
    public static BaseReportingRepo buildBaseReportingRepo(ProcessorContext context) {
        return new AJBaseReportingRepo(context);
    }
    
    public static ReportDataAggregateRepo buildCollectionReportDataAggregateRepo(RDAProcessorContext context) {
        return new AJReportDataAggregateRepo(context);
    }
}
