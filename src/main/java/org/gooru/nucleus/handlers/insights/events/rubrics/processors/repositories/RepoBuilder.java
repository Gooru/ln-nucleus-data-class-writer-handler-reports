package org.gooru.nucleus.handlers.insights.events.rubrics.processors.repositories;

import org.gooru.nucleus.handlers.insights.events.rubrics.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.rubrics.processors.repositories.activejdbc.AJRepoBuilder;

public class RepoBuilder {

    public RubricGradingRepo buildRubricGradingRepo(ProcessorContext context) {
        return AJRepoBuilder.buildRubricsGradingRepo(context);
    }
    
}
