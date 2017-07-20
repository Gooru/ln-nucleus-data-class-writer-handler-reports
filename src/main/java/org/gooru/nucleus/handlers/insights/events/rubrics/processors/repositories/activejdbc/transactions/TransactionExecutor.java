package org.gooru.nucleus.handlers.insights.events.rubrics.processors.repositories.activejdbc.transactions;

import java.sql.SQLException;
import java.util.ResourceBundle;

import org.gooru.nucleus.handlers.insights.events.rubrics.processors.repositories.activejdbc.dbhandlers.DBHandler;
import org.gooru.nucleus.handlers.insights.events.rubrics.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.rubrics.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.rubrics.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.insights.events.app.components.DataSourceRegistry;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ashish on 11/1/16.
 */
public final class TransactionExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionExecutor.class);
    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("messages");

    private TransactionExecutor() {
        throw new AssertionError();
    }

    public static MessageResponse executeTransaction(DBHandler handler) {
        // First validations without any DB
        ExecutionResult<MessageResponse> executionResult = handler.checkSanity();
        // Now we need to run with transaction, if we are going to continue
        if (executionResult.continueProcessing()) {
            executionResult = executeWithTransaction(handler);
        }
        return executionResult.result();

    }

    private static ExecutionResult<MessageResponse> executeWithTransaction(DBHandler handler) {
        ExecutionResult<MessageResponse> executionResult;

        try {
            Base.open(DataSourceRegistry.getInstance().getDefaultDataSource());
            // If we need a read only transaction, then it is time to set up now
            if (handler.handlerReadOnly()) {
                Base.connection().setReadOnly(true);
            }
            Base.openTransaction();
            executionResult = handler.validateRequest();
            if (executionResult.continueProcessing()) {
                executionResult = handler.executeRequest();
                if (executionResult.isSuccessful()) {
                    Base.commitTransaction();
                } else {
                    Base.rollbackTransaction();
                }
            } else {
                Base.rollbackTransaction();
            }
            return executionResult;
        } catch (Throwable e) {
            Base.rollbackTransaction();
            LOGGER.error("Caught exception, need to rollback and abort", e);
            // Most probably we do not know what to do with this, so send
            // internal error
            return new ExecutionResult<>(
                MessageResponseFactory.createInternalErrorResponse(RESOURCE_BUNDLE.getString("error.from.store")),
                ExecutionResult.ExecutionStatus.FAILED);
        } finally {
            if (handler.handlerReadOnly()) {
                // restore the settings
                try {
                    Base.connection().setReadOnly(false);
                } catch (SQLException e) {
                    LOGGER.error("Exception while marking connection to be read/write", e);
                }
            }
            Base.close();
        }
    }
}
