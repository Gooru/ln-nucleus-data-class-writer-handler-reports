package org.gooru.nucleus.handlers.insights.events.rda.processor.collection;

import java.sql.Timestamp;

import org.gooru.nucleus.handlers.insights.events.processors.RDAProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AjEntityCollectionPerformance;
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
public class CollectionStopEventRDAHandler implements DBHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(CollectionStopEventRDAHandler.class);
    private final RDAProcessorContext context;
    private CollectionEventParser event;

    public CollectionStopEventRDAHandler(RDAProcessorContext context) {
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
            event = context.getCollectionEvent();
            if (event.getUser() != null && event.getSessionId() != null && event.getCollectionId() != null) {
                LazyList<AjEntityCollectionPerformance> duplicateRow =
                    AjEntityCollectionPerformance.findBySQL(AjEntityCollectionPerformance.CHECK_DUPLICATE_COLLECTION_EVENT, event.getUser(), event.getSessionId(), event.getCollectionId());
                if (duplicateRow != null && !duplicateRow.isEmpty()) {
                    LOGGER.debug("Found duplicate row in the DB, so updating duplicate row.....");
                    duplicateRow.forEach(dup -> {
                        int id = Integer.valueOf(dup.get(AJEntityReporting.ID).toString());
                        long view = event.getViews();
                        long ts = event.getTimespent();
                        long react = event.getReaction() != 0 ? event.getReaction() : 0;
                        LazyList<AjEntityCollectionPerformance> avgReactionOfItem =
                            AjEntityCollectionPerformance.findBySQL(AJEntityReporting.FETCH_AVG_REACTION_OF_COLL_BY_SESSION, event.getSessionId(), event.getCollectionId());
                        if (avgReactionOfItem != null && !avgReactionOfItem.isEmpty()) {
                            AjEntityCollectionPerformance reaction = avgReactionOfItem.get(0);
                            react = reaction != null ? reaction.getLong(AJEntityReporting.REACTION) : 0;
                        }
                        
                        int updated = Base.exec(AjEntityCollectionPerformance.UPDATE_COLLECTION_METRICS, view, ts, event.getScore(), event.getMaxScore(), react, event.getIsGraded(),
                            new Timestamp(event.getActivityTime()), id);
                        if (updated > 0) {
                            LOGGER.info("Record updated successfully in Coll Perf table");
                        } else {
                            LOGGER.error("Error while updating event into Coll Perf table: " + context.request().toString());
                        }
                    });
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
