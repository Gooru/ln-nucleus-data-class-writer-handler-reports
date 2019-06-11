package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc;

import org.gooru.nucleus.handlers.insights.events.processors.grading.GradingContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.GradingRepo;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.grading.GradingDBHandlerBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.transactions.TransactionExecutor;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;


/**
 * @author mukul@gooru
 */
public class AJGradingRepo implements GradingRepo {
  
  private final GradingContext context;

  public AJGradingRepo(GradingContext context) {
    this.context = context;
  }

  @Override
  public MessageResponse processDCAOASelfGrades() {
    return TransactionExecutor
        .executeTransaction(GradingDBHandlerBuilder.buildDCAOASelfGradingHandler(context));
  }
  
  @Override
  public MessageResponse processDCAOATeacherGrades() {
    return TransactionExecutor
        .executeTransaction(GradingDBHandlerBuilder.buildDCAOATeacherGradingHandler(context));
  }
}