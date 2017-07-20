package org.gooru.nucleus.handlers.insights.events.rubrics.processors.repositories.activejdbc.dbhandlers;

import org.gooru.nucleus.handlers.insights.events.rubrics.processors.ProcessorContext;

/**
 * Created by mukul@gooru
 */

public final class DBHandlerBuilder {

    private DBHandlerBuilder() {
        throw new AssertionError();
    }
    
    public static DBHandler buildRubricGradingHandler(ProcessorContext context) {
        return new RubricGradingHandler(context);
    }
 
}
