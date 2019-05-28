package org.gooru.nucleus.handlers.insights.events.processors.oa;

import java.util.ResourceBundle;
import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
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
public class OAProcessor implements Processor {
  
  private final Message<Object> message;
  private JsonObject request = null;
  private OAContext context;
  private static final Logger LOGGER = LoggerFactory.getLogger(OAProcessor.class);
  private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("messages");

  public OAProcessor(Message<Object> message) {
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
        case MessageConstants.MSG_OP_OA_DCA_EVENT:
          result = createOAEvent();  
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
  
  private MessageResponse createOAEvent() {
    try {
      return RepoBuilder.buildOARepo(context).processOAEvent();
    } catch (Throwable t) {
      LOGGER.error("Exception while processing Collection Play Event Data", t.getMessage());
      return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
    }
  }
  
  private OAContext createContext() {
    return new OAContext(request);
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
