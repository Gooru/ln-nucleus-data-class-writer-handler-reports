package org.gooru.nucleus.handlers.insights.events.processors;

import org.gooru.nucleus.handlers.insights.events.rda.processor.collection.CollectionEventConstants;
import org.gooru.nucleus.handlers.insights.events.rda.processor.collection.CollectionEventProcessor;
import org.gooru.nucleus.handlers.insights.events.rda.processor.resource.ResourceEventConstants;
import org.gooru.nucleus.handlers.insights.events.rda.processor.resource.ResourceEventProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

/**
 * @author renuka@gooru
 *  
 */
public class RDAMessageProcessorBuilder {	
	
	private static final Logger LOGGER = LoggerFactory.getLogger(RDAMessageProcessorBuilder.class);

    private RDAMessageProcessorBuilder() {
        throw new AssertionError();
    }
    
    public static Processor buildKafkaProcessor(JsonObject message) {

        final String eventName = message.getString("eventName");
        switch (eventName) {
        case ResourceEventConstants.EventAttributes.RESOURCE_PERF_EVENT:
            return new ResourceEventProcessor(message);
        case CollectionEventConstants.EventAttributes.COLLECTION_START_EVENT:
        case CollectionEventConstants.EventAttributes.COLLECTION_PERF_EVENT:
        case CollectionEventConstants.EventAttributes.COLLECTION_SCORE_UPDATE_EVENT:
        case CollectionEventConstants.EventAttributes.COLLECTION_SELF_GRADE_EVENT:
            return new CollectionEventProcessor(message);
        default:
            LOGGER.error("Invalid RDA operation type passed in, not able to handle");
            return new CollectionEventProcessor(message);
        }
    }

}
