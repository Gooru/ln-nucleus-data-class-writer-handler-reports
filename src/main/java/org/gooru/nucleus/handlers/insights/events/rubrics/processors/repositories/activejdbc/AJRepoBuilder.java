package org.gooru.nucleus.handlers.insights.events.rubrics.processors.repositories.activejdbc;

import org.gooru.nucleus.handlers.insights.events.rubrics.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.rubrics.processors.repositories.RubricGradingRepo;

public final class AJRepoBuilder {

    private AJRepoBuilder() {
        throw new AssertionError();
    }

    public static RubricGradingRepo buildRubricsGradingRepo(ProcessorContext context) {
        return new AJRubricGradingRepo(context);
    }
 
}
