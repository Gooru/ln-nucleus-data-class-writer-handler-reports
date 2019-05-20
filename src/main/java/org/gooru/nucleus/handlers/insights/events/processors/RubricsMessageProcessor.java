package org.gooru.nucleus.handlers.insights.events.processors;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import java.util.ResourceBundle;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.RepoBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RubricsMessageProcessor implements Processor {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageProcessor.class);
  private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("messages");
  private Message<Object> message = null;
  private String messageObj = null;
  private JsonObject messageJobj = null;
  private String userId;
  private JsonObject request;
  private ProcessorContext context;


  public RubricsMessageProcessor(Message<Object> message) {
    this.message = message;
  }

  public RubricsMessageProcessor(String message) {
    this.messageObj = message;
  }

  public RubricsMessageProcessor(JsonObject message) {
    this.messageJobj = message;
  }


  @Override
  public MessageResponse process() {
    MessageResponse result = null;
    try {
      // Validate the message itself
      ExecutionResult<MessageResponse> validateResult = validateAndInitialize();
      if (validateResult.isCompleted()) {
        return validateResult.result();
      }
      context = createContext();
      //TODO: TOTALLY UPDATE THIS BLOCK
      //CURRENTLY UPDATING THIS CODE TO TEST MY DCA GRADING LOGIC
      //INFRA UPDATES WILL FOLLOW LATER
      final String eventName = messageJobj.getString(EventConstants._EVENT_NAME);
      LOGGER.debug(eventName);
      switch (eventName) {
        //case MessageConstants.MSG_OP_STUDENTS_GRADES_WRITE:
        case EventConstants.RESOURCE_RUBRIC_GRADE:          
          if (messageJobj.containsKey("content_source") && 
              messageJobj.getString("content_source").equalsIgnoreCase(EventConstants.DCA)) {
            result = procStudentDCAGrades();
          } else if (messageJobj.containsKey("content_source") && 
              messageJobj.getString("content_source").equalsIgnoreCase(EventConstants.COURSEMAP)) {
            result = procStudentGrades();            
          } else {
            //Default to CourseMap 
            result = procStudentGrades();   
          }
          break;
        default:
          LOGGER.error("Invalid operation type passed in, not able to handle");
          return MessageResponseFactory
              .createInvalidRequestResponse(RESOURCE_BUNDLE.getString("invalid.operation"));
      }
      return result;
    } catch (Throwable e) {
      LOGGER.error("Unhandled exception in processing", e);
      return MessageResponseFactory.createInternalErrorResponse();
    }
  }


  private MessageResponse procStudentGrades() {
    try {
      return RepoBuilder.buildBaseReportingRepo(context).processStudentGrades();
    } catch (Throwable t) {
      LOGGER.error("Exception while getting Students Grades for a Question", t);
      return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
    }
  }


  private MessageResponse procStudentDCAGrades() {
    try {
      return RepoBuilder.buildBaseReportingRepo(context).processStudentDCAGrades();

    } catch (Throwable t) {
      LOGGER.error("Exception while getting Students Grades for a Question", t);
      return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
    }
  }

  //TODO: This method needs to be updated
  private ProcessorContext createContext() {

    LOGGER.info("context.request(1)" + request);
    return new ProcessorContext(userId, request);
  }

  //This is just the first level validation. Each Individual Handler would need to do more validation based on the
  //handler specific params that are needed for processing.
  private ExecutionResult<MessageResponse> validateAndInitialize() {
    if (message != null) {
      request = ((JsonObject) message.body()).getJsonObject(MessageConstants.MSG_HTTP_BODY);
    }

    if (messageObj != null) {
      request = (new JsonObject(messageObj));
    }

    if (messageJobj != null) {
      request = messageJobj;
    }

    if (request == null) {
      LOGGER.error("Invalid JSON payload on Message Bus");
      return new ExecutionResult<>(MessageResponseFactory
          .createInvalidRequestResponse(RESOURCE_BUNDLE.getString("invalid.payload")),
          ExecutionResult.ExecutionStatus.FAILED);
    }

    // All is well, continue processing
    return new ExecutionResult<>(null, ExecutionResult.ExecutionStatus.CONTINUE_PROCESSING);

  }
}
