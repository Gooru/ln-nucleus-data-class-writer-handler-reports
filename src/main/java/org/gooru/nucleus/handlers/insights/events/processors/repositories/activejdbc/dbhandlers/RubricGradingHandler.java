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
import org.javalite.activejdbc.LazyList;
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
  private Double scoreObj;

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
    
        rubricGrading = new AJEntityRubricGrading();
    
        JsonObject req = context.request();
        
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
    
          Double score = 0.0;
          
          Base.exec(AJEntityReporting.UPDATE_QUESTION_SCORE, rubricGrading.get(AJEntityRubricGrading.STUDENT_SCORE),
                  rubricGrading.get(AJEntityRubricGrading.MAX_SCORE), true, sessionId, rubricGrading.get(AJEntityRubricGrading.RESOURCE_ID));
          
          LOGGER.debug("Computing assessment score...");
          LazyList<AJEntityReporting> scoreTS = AJEntityReporting.findBySQL(AJEntityReporting.COMPUTE_ASSESSMENT_SCORE, rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID), sessionId);
          LOGGER.debug("scoreTS {} ", scoreTS);

          if (!scoreTS.isEmpty()) {
            scoreTS.forEach(m -> {
              scoreObj = Double.valueOf(m.get(AJEntityReporting.SCORE).toString());
              LOGGER.debug("scoreObj {} ", scoreObj);
    
            });
            Object qc = Base.firstCell(AJEntityReporting.GET_QUESTION_COUNT_SESS, rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID), sessionId);
            int questionCount = qc !=null ? (((Number) qc).intValue()) : 0;
            LOGGER.debug("Question Count {} ", qc);    
            if (scoreObj != null && questionCount != 0) {
              scoreObj = ((scoreObj * 100) / ((Number) qc).intValue());
              LOGGER.debug("ReCalculted assessment score {} ", scoreObj);
            } else {
              scoreObj = 0.0;
            }
    
          }
          Base.exec(AJEntityReporting.UPDATE_ASSESSMENT_SCORE, scoreObj, rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID), sessionId);
          LOGGER.debug("Assessment score updated successfully...");
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
