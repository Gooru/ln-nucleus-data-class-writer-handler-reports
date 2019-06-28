package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc;

import org.gooru.nucleus.handlers.insights.events.processors.oa.OAContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.OARepo;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.oa.OADBHandlerBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.transactions.TransactionExecutor;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;

public class AJOARepo implements OARepo {
  
  private final OAContext context;

  public AJOARepo(OAContext context) {
    this.context = context;
  }

  @Override
  public MessageResponse processOAEvent() {
    return TransactionExecutor
        .executeTransaction(OADBHandlerBuilder.buildOAEventHandler(context));
  }

  @Override
  public MessageResponse processOASelfGrades() {
    return TransactionExecutor
        .executeTransaction(OADBHandlerBuilder.buildOASelfGradeHandler(context));
  }
  
  @Override
  public MessageResponse storeSubmissionDetails() {
    return TransactionExecutor
        .executeTransaction(OADBHandlerBuilder.buildOASubmissionsHandler(context));
  }
  
  @Override
  public MessageResponse processOACompletionEvent() {
    return TransactionExecutor
        .executeTransaction(OADBHandlerBuilder.buildOACompletionEventHandler(context));
  }
  
}
