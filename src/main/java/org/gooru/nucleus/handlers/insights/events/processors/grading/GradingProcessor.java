package org.gooru.nucleus.handlers.insights.events.processors.grading;

import java.util.ResourceBundle;
import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.processors.Processor;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.RepoBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;


/**
 * @author mukul@gooru
 */
public class GradingProcessor implements Processor {
  private final Message<Object> message;
  private JsonObject request = null;
  private GradingContext context;
  private static final Logger LOGGER = LoggerFactory.getLogger(GradingProcessor.class);
  private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("messages");

  public GradingProcessor(Message<Object> message) {
    this.message = message;
  }
  
  @Override
  public MessageResponse process() {
    MessageResponse result;
    String msgop = message.headers().get(MessageConstants.MSG_HEADER_OP);
    ExecutionResult<MessageResponse> validateResult = validateRequest();
    if (validateResult.continueProcessing()) {
      context = createContext();
      switch (msgop) {
        case MessageConstants.MSG_OP_RUBRIC_GRADING:
          result = processRubricGrades();  
          break;
        default:
          LOGGER.warn("Invalid op: '{}'", String.valueOf(msgop));
          return MessageResponseFactory.createInvalidRequestResponse();
      }
      return result;
    } else {
      return MessageResponseFactory.createInvalidRequestResponse();      
    }
    
  }
  
  private MessageResponse processRubricGrades() {
    MessageResponse response = null;
    
    Grader grader = null;
    String graderType = context.request().getString(MessageConstants.GRADER);
    switch (graderType) {
      case MessageConstants.GRADER_SELF:
        grader = new SelfGrader();
        break;
      case MessageConstants.GRADER_TEACHER:
        grader = new TeacherGrader();
        break;
      case MessageConstants.GRADER_PEER:
        grader = new PeerGrader();
        break;
      default:
        LOGGER.warn("Invalid Grading Type: '{}'", grader);
    }
    
    if( grader != null) {
      grader.setContext(context);
      response = grader.process();
    }else {
      response =  MessageResponseFactory.createInvalidRequestResponse();
    }
    return response;
  }
    
  private GradingContext createContext() {
    return new GradingContext(request);
  }

  private ExecutionResult<MessageResponse> validateRequest() {

    if (message != null) {
      request = ((JsonObject) message.body()).getJsonObject(MessageConstants.MSG_HTTP_BODY);
    }   

    if (request == null) {
      LOGGER.error("Invalid JSON payload on Message Bus");
      return new ExecutionResult<>(MessageResponseFactory
          .createInvalidRequestResponse(RESOURCE_BUNDLE.getString("invalid.payload")),
          ExecutionResult.ExecutionStatus.FAILED);
    }
    // All'z well!, continue..
    return new ExecutionResult<>(null, ExecutionResult.ExecutionStatus.CONTINUE_PROCESSING);
  }
}
