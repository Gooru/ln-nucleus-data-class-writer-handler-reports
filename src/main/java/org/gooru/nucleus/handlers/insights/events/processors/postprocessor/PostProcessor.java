package org.gooru.nucleus.handlers.insights.events.processors.postprocessor;

import static org.gooru.nucleus.handlers.insights.events.constants.MessageConstants.MSG_OP_DIAGNOSTIC_ASMT;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.gooru.nucleus.handlers.insights.events.processors.Processor;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ashish.
 */

public class PostProcessor implements Processor {

  private final Message<JsonObject> message;
  private static final Logger LOGGER = LoggerFactory.getLogger(PostProcessor.class);

  public PostProcessor(Message<JsonObject> message) {
    this.message = message;
  }

  // Return value does not matter
  @Override
  public MessageResponse process() {
    String msgop = message.headers().get(MessageConstants.MSG_HEADER_OP);
    switch (msgop) {
      case MSG_OP_DIAGNOSTIC_ASMT:
        PostProcessorHandler.buildDiagnosticAssessmentPlayedHandler(message.body()).handle();
        break;
      default:
        LOGGER.warn("Invalid op: '{}'", String.valueOf(msgop));
    }
    return MessageResponseFactory.createNoContentResponse();
  }
}
