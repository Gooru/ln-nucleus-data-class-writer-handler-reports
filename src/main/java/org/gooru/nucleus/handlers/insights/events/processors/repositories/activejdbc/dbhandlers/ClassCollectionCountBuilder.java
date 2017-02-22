package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.events.EventParser;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityClassCollectionCount;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author daniel
 */
public class ClassCollectionCountBuilder implements DBHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClassCollectionCountBuilder.class);
  private final ProcessorContext context;
  private EventParser event;

  public ClassCollectionCountBuilder(ProcessorContext context) {
      this.context = context;
  }
  
  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("invalid request received");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse("Invalid data received to process events"),
          ExecutionStatus.FAILED);
  }

  LOGGER.debug("checkSanity() OK");
  return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
}

  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
}

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    //FIXME : Revisit this logic once we construct the event structure.
    Base.exec(AJEntityClassCollectionCount.INSERT_CLASS_COLLECTION_COUNT,event.getClassGooruId(), event.getCourseGooruId(), event.getUnitGooruId(), event.getLessonGooruId(),0,0,0);;
    return new ExecutionResult<>(MessageResponseFactory.createCreatedResponse(), ExecutionStatus.SUCCESSFUL);
  }

  @Override
  public boolean handlerReadOnly() {
    // TODO Auto-generated method stub
    return false;
  }

}
