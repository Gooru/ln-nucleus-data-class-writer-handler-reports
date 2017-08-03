package org.gooru.nucleus.handlers.insights.events.processors;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

public final class ProcessorBuilder {

    private ProcessorBuilder() {
        throw new AssertionError();
    }

    public static Processor build(Message<Object> message) {
        return new MessageProcessor(message);
    }
    
    public static Processor build(JsonObject message) {
      return new MessageProcessor(message);
    }
}
