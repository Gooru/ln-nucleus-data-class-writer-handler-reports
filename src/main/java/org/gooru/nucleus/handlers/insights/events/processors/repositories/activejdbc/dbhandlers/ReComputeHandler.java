package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.events.EventParser;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReComputeHandler implements DBHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReComputeHandler.class);
  private final ProcessorContext context;
  private EventParser event;

  public ReComputeHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("invalid request received");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid data received to process events"),
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
    event = context.getEvent();
    String query = null;
    switch (event.getContentFormat()) {
    case MessageConstants.COURSE:
      query = AJEntityReporting.DELETE_BASEREPORT_BY_COURSE;
      break;
    case MessageConstants.UNIT:
      query = AJEntityReporting.DELETE_BASEREPORT_BY_UNIT;
      break;
    case MessageConstants.LESSON:
      query = AJEntityReporting.DELETE_BASEREPORT_BY_LESSON;
      break;
    case MessageConstants.COLLECTION:
      query = AJEntityReporting.DELETE_BASEREPORT_BY_COLLECTION;
      break;
    case MessageConstants.ASSESSMENT:
      query = AJEntityReporting.DELETE_BASEREPORT_BY_COLLECTION;
      break;
    default:
      LOGGER.info("Invalid content format..");
    }
    if (query != null) {
      Base.exec(query, event.getClassGooruId(), event.getContentGooruId());
    } else {
      LOGGER.warn("Nothing to process");
    }
    LOGGER.debug("Deleted record for class : {} - contentFormat : {} - content : {} ", event.getClassGooruId(), event.getContentFormat(),
            event.getContentGooruId());
    return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(), ExecutionStatus.SUCCESSFUL);
  }

  @Override
  public boolean handlerReadOnly() {
    // TODO Auto-generated method stub
    return false;
  }

}
