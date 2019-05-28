package org.gooru.nucleus.handlers.insights.events.processors.oa;

import org.gooru.nucleus.handlers.insights.events.processors.repositories.OARepo;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.oa.OADBHandlerBuilder;
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
  
}
