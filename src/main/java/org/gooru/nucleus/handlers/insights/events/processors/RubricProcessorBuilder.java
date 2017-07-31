package org.gooru.nucleus.handlers.insights.events.processors;

import io.vertx.core.json.JsonObject;

public class RubricProcessorBuilder {
  private RubricProcessorBuilder() {
    throw new AssertionError();
}

public static Processor build(JsonObject message) {
  return new RubricsMessageProcessor(message);
}
}
