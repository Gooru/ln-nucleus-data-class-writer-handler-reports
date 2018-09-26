package org.gooru.nucleus.handlers.insights.events.rda.processor.resource;

import java.util.ResourceBundle;

import org.gooru.nucleus.handlers.insights.events.processors.Processor;
import org.gooru.nucleus.handlers.insights.events.processors.RDAProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.RepoBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

/**
 * @author renuka@gooru
 * 
 */
public class ResourceEventProcessor implements Processor {

    private final JsonObject message;
    private JsonObject request;
    private RDAProcessorContext context;
    private ResourceEventParser event;
    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceEventProcessor.class);
    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("messages");

    public ResourceEventProcessor(JsonObject message) {
        this.message = message;
    }

    @Override
    public MessageResponse process() {

        MessageResponse result;

        try {
            // Validate the message itself
            ExecutionResult<MessageResponse> validateResult = validateAndInitialize();
            if (validateResult.isCompleted()) {
                return validateResult.result();
            }
            context = createContext();

            switch (event.getEventName()) {
            case ResourceEventConstants.EventAttributes.RESOURCE_PERF_EVENT:
                result = updateCollectionTimespent();
                LOGGER.info("RDA: Resource Stop Event successfully processed");
                break;
            default:
                LOGGER.error("Invalid operation type passed in, not able to handle");
                return MessageResponseFactory.createInvalidRequestResponse(RESOURCE_BUNDLE.getString("invalid.operation"));
            }

            return result;
        } catch (Throwable e) {
            LOGGER.warn("Encountered error while processing Resource event for RDA", e);
            return MessageResponseFactory.createInternalErrorResponse();
        }
    }

    private MessageResponse updateCollectionTimespent() {
        try {
            return RepoBuilder.buildReportDataAggregateRepo(context).processResourceStopDataForRDA();
        } catch (Throwable t) {
            LOGGER.error("Exception while processing Student Self Reporting Score Data ", t.getMessage());
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }
    }

    private RDAProcessorContext createContext() {
        try {
            event = new ResourceEventParser(request.toString());
        } catch (Exception e) {
            LOGGER.error("Error parsing event");
            e.printStackTrace();
        }
        return new RDAProcessorContext(request, event);
    }

    private ExecutionResult<MessageResponse> validateAndInitialize() {

        if (message != null) {
            request = message;
        }
        if (request == null) {
            LOGGER.error("Invalid JSON payload on Message Bus");
            return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(RESOURCE_BUNDLE.getString("invalid.payload")), ExecutionResult.ExecutionStatus.FAILED);
        }
        // All is well, continue processing
        return new ExecutionResult<>(null, ExecutionResult.ExecutionStatus.CONTINUE_PROCESSING);
    }

}
