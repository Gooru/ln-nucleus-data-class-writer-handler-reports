package org.gooru.nucleus.handlers.insights.events.processors.oa;

import org.gooru.nucleus.handlers.insights.events.processors.repositories.OARepo;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.oa.OADBHandlerBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.oa.OASubmissionsHandler;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.transactions.TransactionExecutor;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;


/**
 * @author mukul@gooru
 */
public class OAProcessorBuilder implements OARepo {

  private final OAContext context;

  public OAProcessorBuilder(OAContext context) {
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
  
}
