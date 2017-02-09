package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.events.EventParser;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityTaxonomyReporting;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author insightsTeam
 */
class TaxonomyReportHandler implements DBHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(TaxonomyReportHandler.class);
    private final ProcessorContext context;
    private EventParser event;

    public TaxonomyReportHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("invalid request received");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid data received to process events"),
                ExecutionStatus.FAILED);
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
      event = context.getEvent();
      PreparedStatement ps = Base.startBatch(AJEntityTaxonomyReporting.INSERT_TAXONOMY_REPORT);
      Object maxSequenceId = Base.firstCell(AJEntityTaxonomyReporting.SELECT_TAXONOMY_REPORT_MAX_SEQUENCE_ID);
      int sequenceId = 1;
      if (maxSequenceId != null) {
        sequenceId = Integer.valueOf(maxSequenceId.toString()) + 1;
      }
      for (String internalTaxonomyCode : event.getTaxonomyIds().fieldNames()) {
        String displayCode = event.getTaxonomyIds().getString(internalTaxonomyCode);
        Map<String, String> taxObject = new HashMap<>();
        splitByTaxonomyCode(internalTaxonomyCode, taxObject);
        Base.addBatch(ps, sequenceId, event.getSessionId(), event.getGooruUUID(), taxObject.get(MessageConstants.SUBJECT),
                taxObject.get(MessageConstants.COURSE), taxObject.get(MessageConstants.DOMAIN), taxObject.get(MessageConstants.STANDARDS),
                taxObject.containsKey(MessageConstants.LEARNING_TARGETS) ? taxObject.get(MessageConstants.LEARNING_TARGETS) : EventConstants.NA, displayCode, event.getParentGooruId(), event.getContentGooruId(),
                event.getResourceType(), event.getQuestionType(), event.getAnswerObject().toString(), event.getAnswerStatus(), 1, 0, event.getScore(),
                event.getTimespent());
        sequenceId = sequenceId + 1;
      }
      Base.executeBatch(ps);
      LOGGER.debug("Taxonomy report data inserted successfully:" + event.getSessionId());
      try {
        ps.close();
      } catch (SQLException e) {
        LOGGER.error("SQL exception while inserting event: {}", e);
        return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(), ExecutionStatus.FAILED);
      }
      return new ExecutionResult<>(MessageResponseFactory.createCreatedResponse(), ExecutionStatus.SUCCESSFUL);
    }
    
    @Override
    public boolean handlerReadOnly() {
        return false;
    }
    
    private void splitByTaxonomyCode(String taxonomyCode, Map<String, String> taxObject){
      int index = 0;
      for(String value : taxonomyCode.split(MessageConstants.HYPHEN)){
           switch(index){
           case 0:
             taxObject.put(MessageConstants.SUBJECT, value);
             break;
           case 1:
             taxObject.put(MessageConstants.COURSE, value);
             break;
           case 2:
             taxObject.put(MessageConstants.DOMAIN, value);
             break;
           case 3:
             taxObject.put(MessageConstants.STANDARDS, value);
             break;
           case 4:
             taxObject.put(MessageConstants.LEARNING_TARGETS, value);
             break;
           }
           index++;
         }
    }
}
