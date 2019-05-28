package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc;

import org.gooru.nucleus.handlers.insights.events.processors.RDAProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.oa.OAContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.OARepo;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.oa.OADBHandlerBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.transactions.TransactionExecutor;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import io.vertx.core.json.JsonObject;

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

}
