package org.gooru.nucleus.handlers.insights.events.processors;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.gooru.nucleus.handlers.insights.events.processors.oa.OAProcessor;
import org.gooru.nucleus.handlers.insights.events.processors.postprocessor.PostProcessor;

public final class ProcessorBuilder {

  private ProcessorBuilder() {
    throw new AssertionError();
  }

  public static Processor build(JsonObject message) {
    return new MessageProcessor(message);
  }

  public static Processor buildRubrics(JsonObject message) {
    return new RubricsMessageProcessor(message);
  }

  public static Processor buildUpdater(Message<Object> message) {
    return new UpdateMessageProcessor(message);
  }

  public static Processor buildSelfReportingProcessor(Message<Object> message) {
    return new SelfReportingProcessor(message);
  }

  public static Processor buildOfflineStudentReportingProcessor(Message<Object> message) {
    return new OfflineStudentReportingProcessor(message);
  }

  public static Processor buildPostProcessor(Message<JsonObject> message) {
    return new PostProcessor(message);
  }
  
  public static Processor buildOAProcessor(Message<Object> message) {
    return new OAProcessor(message);
  }
  
}
