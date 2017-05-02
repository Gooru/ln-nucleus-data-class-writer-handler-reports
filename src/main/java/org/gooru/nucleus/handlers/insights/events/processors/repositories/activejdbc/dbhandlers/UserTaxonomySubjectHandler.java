package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.events.EventParser;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityUserTaxonomySubject;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.internal.StringUtil;
import io.vertx.core.impl.StringEscapeUtils;

/**
 * 
 * @author daniel
 *
 */
public class UserTaxonomySubjectHandler implements DBHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(UserTaxonomySubjectHandler.class);
  private final ProcessorContext context;
  private AJEntityUserTaxonomySubject userTaxonomySubject;
  private EventParser event;

  public UserTaxonomySubjectHandler(ProcessorContext context) {
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
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {
    event = context.getEvent();
    if (!StringUtil.isNullOrEmpty(event.getCourseGooruId())) {
      Object taxSubjectId = Base.firstCell(AJEntityUserTaxonomySubject.SELECT_SUBJECT_ID_BY_COURSE, event.getCourseGooruId());
      if (taxSubjectId != null) {
        userTaxonomySubject = new AJEntityUserTaxonomySubject();
        userTaxonomySubject.set(AJEntityUserTaxonomySubject.COURSE_ID, event.getCourseGooruId());
        userTaxonomySubject.set(AJEntityUserTaxonomySubject.ACTOR_ID, event.getGooruUUID());
        // Get Subject ID for this course
        userTaxonomySubject.set(AJEntityUserTaxonomySubject.TAX_SUBJECT_ID, taxSubjectId);

        if (userTaxonomySubject.isValid()) {
          if (userTaxonomySubject.insert()) {
            LOGGER.info("Successfully inserted in UserTaxonomySubject");
          } else {
            LOGGER.error("Error while inserting in UserTaxonomySubject :" + context.request().toString());
          }
        } else {
          LOGGER.warn("Event validation error");
          return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(), ExecutionStatus.FAILED);

        }
      } else {
        LOGGER.debug("Don't process if tax subject id is NULL");
      }
    } else {
      LOGGER.debug("Don't process if couse id is NULL");
    }
    return new ExecutionResult<>(MessageResponseFactory.createCreatedResponse(), ExecutionStatus.SUCCESSFUL);
  }

  @Override
  public boolean handlerReadOnly() {
    // TODO Auto-generated method stub
    return false;
  }

}
