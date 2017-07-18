package org.gooru.nucleus.handlers.insights.events.rubrics.processors.repositories.activejdbc;

import org.gooru.nucleus.handlers.insights.events.rubrics.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.rubrics.processors.repositories.RubricGradingRepo;
import org.gooru.nucleus.handlers.insights.events.rubrics.processors.repositories.activejdbc.dbhandlers.DBHandlerBuilder;
import org.gooru.nucleus.handlers.insights.events.rubrics.processors.repositories.activejdbc.transactions.TransactionExecutor;
import org.gooru.nucleus.handlers.insights.events.rubrics.processors.responses.MessageResponse;

/**
 * Created by mukul@gooru
 */
class AJRubricGradingRepo implements RubricGradingRepo {
    private final ProcessorContext context;

    public AJRubricGradingRepo(ProcessorContext context) {
        this.context = context;
    }
 
     
    @Override
    public MessageResponse processStudentGrades() {
        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildRubricGradingHandler(context));
    }

}
