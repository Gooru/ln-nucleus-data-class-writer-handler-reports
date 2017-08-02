package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.util.Map;

import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityRubricGrading;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.EntityBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;


/**
 * Created by mukul@gooru
 * 
 */

public class RubricGradingHandler implements DBHandler {

	
	private static final Logger LOGGER = LoggerFactory.getLogger(RubricGradingHandler.class);
        
	private final ProcessorContext context;
    private AJEntityRubricGrading rubricGrading;

    public RubricGradingHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
         if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("Invalid Rubric Grading Data");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid Rubric Grading Data"),
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
    
        LOGGER.info("Rubric Grading Infra Setup");
        LOGGER.info("context.request" + context.request());
    
        rubricGrading = new AJEntityRubricGrading();
    
        JsonObject req = context.request();
        LOGGER.info(req.encodePrettily());
    
        new DefAJEntityRubricGradingEntityBuilder().build(rubricGrading, context.request(), AJEntityRubricGrading.getConverterRegistry());
        boolean result = rubricGrading.save();
        if (!result) {
          LOGGER.error("Grades cannot be inserted into the DB for the student " + context.request().getValue(AJEntityRubricGrading.STUDENT_ID));
          if (rubricGrading.hasErrors()) {
            Map<String, String> map = rubricGrading.errors();
            JsonObject errors = new JsonObject();
            map.forEach(errors::put);
            return new ExecutionResult<>(MessageResponseFactory.createValidationErrorResponse(errors), ExecutionResult.ExecutionStatus.FAILED);
          }
        }
        LOGGER.debug("Student Rubric grades stored successfully for the student " + context.request().getValue(AJEntityRubricGrading.STUDENT_ID));
        Object sessionId = rubricGrading.get(AJEntityRubricGrading.SESSION_ID);
        if (sessionId == null) {
          sessionId = Base.firstCell(AJEntityReporting.FIND_SESSION_ID, rubricGrading.get(AJEntityRubricGrading.CLASS_ID),
                  rubricGrading.get(AJEntityRubricGrading.COURSE_ID), rubricGrading.get(AJEntityRubricGrading.UNIT_ID),
                  rubricGrading.get(AJEntityRubricGrading.LESSON_ID), rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID),
                  rubricGrading.get(AJEntityRubricGrading.RESOURCE_ID));
          LOGGER.debug("Lookup : " + sessionId);
        }
        if (sessionId != null) {
          LOGGER.debug("Student Score : {} ", rubricGrading.get(AJEntityRubricGrading.STUDENT_SCORE));
          LOGGER.debug("Max Score : {} ", rubricGrading.get(AJEntityRubricGrading.MAX_SCORE));
          LOGGER.debug("session id : {} ", sessionId);
          LOGGER.debug("resource id : {} ", rubricGrading.get(AJEntityRubricGrading.RESOURCE_ID));
    
          Base.exec(AJEntityReporting.UPDATE_SCORE, rubricGrading.get(AJEntityRubricGrading.STUDENT_SCORE),
                  rubricGrading.get(AJEntityRubricGrading.MAX_SCORE), sessionId, rubricGrading.get(AJEntityRubricGrading.RESOURCE_ID));
        }
        LOGGER.info("DONE");
        return new ExecutionResult<>(MessageResponseFactory.createCreatedResponse(), ExecutionStatus.SUCCESSFUL);
      }   
    
    private static class DefAJEntityRubricGradingEntityBuilder implements EntityBuilder<AJEntityRubricGrading> {
    }
    

    @Override
    public boolean handlerReadOnly() {
        return false;
    }
       

}
