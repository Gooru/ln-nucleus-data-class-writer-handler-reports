package org.gooru.nucleus.handlers.insights.events.rda.processor.collection;

import java.util.ResourceBundle;

import org.gooru.nucleus.handlers.insights.events.processors.RDAProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.Processor;
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
public class CollectionEventProcessor implements Processor {

    private final JsonObject message;
    private JsonObject request;
    private RDAProcessorContext context;
    private CollectionEventParser event;
    private static final Logger LOGGER = LoggerFactory.getLogger(CollectionEventProcessor.class);
    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("messages");

    public CollectionEventProcessor(JsonObject message) {
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
            final String eventName = event.getEventName();
            switch (eventName) {
            case CollectionEventConstants.EventAttributes.COLLECTION_START_EVENT:
                result = processCollectionStartDataForRDA();
                LOGGER.info("RDA: Collection Start Event successfully processed");
                break;
            case CollectionEventConstants.EventAttributes.COLLECTION_PERF_EVENT:
                result = processCollectionStopDataForRDA();
                LOGGER.info("RDA: Collection Performance Event successfully processed");
                break;
            case CollectionEventConstants.EventAttributes.COLLECTION_SCORE_UPDATE_EVENT:
                result = processCollScoreUpdateDataForRDA();
                LOGGER.info("RDA: Collection Score Update Event successfully processed");
                break;
            case CollectionEventConstants.EventAttributes.COLLECTION_SELF_GRADE_EVENT:
                result = processStudentSelfGradeDataForRDA();
                LOGGER.info("RDA: Student Self Grade Event successfully processed");
                break;
            default:
                LOGGER.error("Invalid operation type passed in, not able to handle");
                return MessageResponseFactory.createInvalidRequestResponse(RESOURCE_BUNDLE.getString("invalid.operation"));
            }
            return result;

        } catch (Throwable e) {
            LOGGER.warn("Encountered error while processing Collection Events for RDA", e);
            return MessageResponseFactory.createInternalErrorResponse();
        }
    }

    private MessageResponse processCollectionStartDataForRDA() {
        try {
            return RepoBuilder.buildReportDataAggregateRepo(context).processCollectionStartDataForRDA();
        } catch (Throwable t) {
            LOGGER.error("Exception while processing Collection start event for RDA", t.getMessage());
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }
    }

    private MessageResponse processCollectionStopDataForRDA() {
        try {
            return RepoBuilder.buildReportDataAggregateRepo(context).processCollectionStopDataForRDA();
        } catch (Throwable t) {
            LOGGER.error("Exception while processing Collection stop event for RDA ", t.getMessage());
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }
    }

    private MessageResponse processCollScoreUpdateDataForRDA() {
        try {
            return RepoBuilder.buildReportDataAggregateRepo(context).processCollScoreUpdateDataForRDA();
        } catch (Throwable t) {
            LOGGER.error("Exception while processing Collection stop event for RDA ", t.getMessage());
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }
    }
    
    private MessageResponse processStudentSelfGradeDataForRDA() {
        try {
            return RepoBuilder.buildReportDataAggregateRepo(context).processStudentSelfGradeDataForRDA();
        } catch (Throwable t) {
            LOGGER.error("Exception while processing Student Self Grade event for RDA ", t.getMessage());
            return MessageResponseFactory.createInternalErrorResponse(t.getMessage());
        }
    }

    private RDAProcessorContext createContext() {
        try {
            event = new CollectionEventParser(request.toString());
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
