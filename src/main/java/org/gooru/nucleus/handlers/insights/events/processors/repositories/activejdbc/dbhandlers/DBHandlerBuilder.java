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
     
    public static DBHandler buildTaxonomyReportHandler(ProcessorContext context) {
      return new TaxonomyReportHandler(context);
    }
}
