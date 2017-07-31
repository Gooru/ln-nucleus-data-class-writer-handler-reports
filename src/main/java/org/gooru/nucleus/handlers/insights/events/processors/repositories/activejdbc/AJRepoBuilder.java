package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc;

import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.BaseReportingRepo;

public class AJRepoBuilder {
    
    public static BaseReportingRepo buildBaseReportingRepo(ProcessorContext context) {
        return new AJBaseReportingRepo(context);
    }
}
