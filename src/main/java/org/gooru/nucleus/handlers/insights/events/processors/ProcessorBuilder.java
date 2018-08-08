package org.gooru.nucleus.handlers.insights.events.processors;

import io.vertx.core.json.JsonObject;
import io.vertx.core.eventbus.Message;

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

    public static Processor buildSelfReportingProcessor (Message<Object> message) {
        return new SelfReportingProcessor(message);
    }

}
