package org.gooru.nucleus.handlers.insights.events.rda.processor.resource;

import java.sql.Timestamp;

import org.gooru.nucleus.handlers.insights.events.processors.RDAProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityCollectionPerformance;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author renuka@gooru
 * 
 */
public class ResourceStopEventRDAHandler implements DBHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceStopEventRDAHandler.class);

    private final RDAProcessorContext context;
    private ResourceEventParser resourceEvent;

    public ResourceStopEventRDAHandler(RDAProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("invalid request received");
            return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid data received to process events"), ExecutionStatus.FAILED);
        }

        LOGGER.debug("checkSanity() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    public ExecutionResult<MessageResponse> validateRequest() {
        LOGGER.debug("validateRequest() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    public ExecutionResult<MessageResponse> executeRequest() {
        try {
            resourceEvent = context.getResourceEvent();
            if (resourceEvent.getCollectionId() != null) {
                LazyList<AJEntityCollectionPerformance> duplicateRow = AJEntityCollectionPerformance.findBySQL(AJEntityCollectionPerformance.CHECK_DUPLICATE_COLLECTION_EVENT, resourceEvent.getUser(),
                    resourceEvent.getSessionId(), resourceEvent.getCollectionId());
                if (duplicateRow != null && !duplicateRow.isEmpty()) {
                    LOGGER.debug("Found duplicate row in the DB, so updating duplicate row.....");
                    duplicateRow.forEach(dup -> {
                        int id = Integer.valueOf(dup.get("id").toString());
                        long ts = (Long.valueOf(dup.get("timespent").toString()) + resourceEvent.getTimeSpent());
                        Base.exec(AJEntityCollectionPerformance.UPDATE_COLLECTION_TIMESPENT, ts, new Timestamp(resourceEvent.getActivityTime()), id);
                    });
                } else {
                    LOGGER.warn("RDA-RSE:: Update Skipped! coll start event either missing or some issue. RDA-EVENT: {}", context.request());
                }
            }
        } catch (Exception e) {
            LOGGER.error("EXCEPTION::::", e);
            return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(), ExecutionStatus.FAILED);
        }
        return new ExecutionResult<>(MessageResponseFactory.createCreatedResponse(), ExecutionStatus.SUCCESSFUL);

    }

    @Override
    public boolean handlerReadOnly() {
        return false;
    }

}
