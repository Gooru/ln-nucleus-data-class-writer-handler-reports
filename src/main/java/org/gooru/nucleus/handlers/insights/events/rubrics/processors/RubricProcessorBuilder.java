package org.gooru.nucleus.handlers.insights.events.rubrics.processors;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

public final class RubricProcessorBuilder {

    private RubricProcessorBuilder() {
        throw new AssertionError();
    }

    public static RubricProcessor build(Message<Object> message) {
        return new MessageProcessor(message);
    }
    
    public static RubricProcessor build(JsonObject message) {
        return new MessageProcessor(message);
      }
    
    
}
