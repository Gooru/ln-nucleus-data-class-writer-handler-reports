package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
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
  private Double score;
  private Double max_score;

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
    	  //TODO: Update Teacher Validation Logic
//        if (context.getUserIdFromRequest() != null) {
//          List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, class_id, this.context.userIdFromSession());
//          if (owner.isEmpty()) {
//            LOGGER.debug("validateRequest() FAILED");
//            return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
//          }
//        }    	
    	LOGGER.debug("validateRequest() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
      public ExecutionResult<MessageResponse> executeRequest() {
    
        rubricGrading = new AJEntityRubricGrading();    
        JsonObject req = context.request();        
        LazyList<AJEntityReporting> duplicateRow = null;
        
        new DefAJEntityRubricGradingEntityBuilder().build(rubricGrading, context.request(), AJEntityRubricGrading.getConverterRegistry());
        Object collType = Base.firstCell(AJEntityReporting.FIND_COLLECTION_TYPE, rubricGrading.get(AJEntityRubricGrading.CLASS_ID),
                rubricGrading.get(AJEntityRubricGrading.COURSE_ID), rubricGrading.get(AJEntityRubricGrading.UNIT_ID),
                rubricGrading.get(AJEntityRubricGrading.LESSON_ID), rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID));
        LOGGER.debug("Collection Type " + collType);
        
        //Currently this situation may arise if the teacher decides to change the Grades in the current Rubric Grading session
        //using the Prev-Next buttons (ref: UI). So the Rubric associated to a question will always be the same.
        //However teacher only gets One Attempt at Grading. (i.e once this Rubric Grading session is closed by the teacher, 
        // the grades cannot be changed)
        duplicateRow =  AJEntityReporting.findBySQL(AJEntityRubricGrading.THIS_QUESTION_GRADES_FIND, 
        		rubricGrading.get(AJEntityRubricGrading.STUDENT_ID), rubricGrading.get(AJEntityRubricGrading.CLASS_ID),
                rubricGrading.get(AJEntityRubricGrading.COURSE_ID), rubricGrading.get(AJEntityRubricGrading.UNIT_ID),
                rubricGrading.get(AJEntityRubricGrading.LESSON_ID), rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID),
                rubricGrading.get(AJEntityRubricGrading.RESOURCE_ID), rubricGrading.get(AJEntityRubricGrading.SESSION_ID));
        
        if (duplicateRow == null || duplicateRow.isEmpty()) {
        	if (collType != null){
            	rubricGrading.set("collection_type", collType.toString());        	
            }
            rubricGrading.set("grader", "Teacher");
            //Timestamps are mandatory 
            rubricGrading.set("created_at", new Timestamp(Long.valueOf(rubricGrading.get(AJEntityRubricGrading.CREATED_AT).toString())));
            rubricGrading.set("updated_at", new Timestamp(Long.valueOf(rubricGrading.get(AJEntityRubricGrading.UPDATED_AT).toString())));
            
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
        } else if (duplicateRow != null) {
        	LOGGER.debug("Found duplicate row in the DB, this question appears to be graded previously, will be updated");
            duplicateRow.forEach(dup -> {
                int id = Integer.valueOf(dup.get("id").toString());
                //update the Answer Object and Answer Status from the latest event
                Base.exec(AJEntityRubricGrading.UPDATE_QUESTION_GRADES, rubricGrading.get(AJEntityRubricGrading.STUDENT_SCORE),
                		rubricGrading.get(AJEntityRubricGrading.MAX_SCORE), rubricGrading.get(AJEntityRubricGrading.OVERALL_COMMENT),
                		rubricGrading.get(AJEntityRubricGrading.CATEGORY_SCORE), 
                		new Timestamp(Long.valueOf(rubricGrading.get(AJEntityRubricGrading.UPDATED_AT).toString())), id);
              });
        }
        
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
          
          Base.exec(AJEntityReporting.UPDATE_QUESTION_SCORE, rubricGrading.get(AJEntityRubricGrading.STUDENT_SCORE),
                  rubricGrading.get(AJEntityRubricGrading.MAX_SCORE), true, sessionId.toString(), rubricGrading.get(AJEntityRubricGrading.RESOURCE_ID));
          
          if (collType.toString().equalsIgnoreCase(AJEntityRubricGrading.ATTR_ASSESSMENT)) {
        	  LOGGER.debug("Computing assessment score...");
              LazyList<AJEntityReporting> scoreTS = AJEntityReporting.findBySQL(AJEntityReporting.COMPUTE_ASSESSMENT_SCORE_POST_GRADING, 
            		  rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID), sessionId);
              LOGGER.debug("scoreTS {} ", scoreTS);

              if (scoreTS != null && !scoreTS.isEmpty()) {
                scoreTS.forEach(m -> {
                  score = (m.get(AJEntityReporting.SCORE) != null ? Double.valueOf(m.get(AJEntityReporting.SCORE).toString()) : null);
                  LOGGER.debug("score {} ", score);
                  max_score = (m.get(AJEntityReporting.MAX_SCORE) != null ? Double.valueOf(m.get(AJEntityReporting.MAX_SCORE).toString()) : null);
                  LOGGER.debug("max_score {} ", max_score);        
                });
                            
                if (score != null && max_score != null && max_score != 0.0) {
                  score = ((score * 100) / max_score);
                  LOGGER.debug("Re-Computed assessment score {} ", score);
                }
              }
              Base.exec(AJEntityReporting.UPDATE_ASSESSMENT_SCORE, score, max_score, rubricGrading.get(AJEntityRubricGrading.COLLECTION_ID), sessionId);
              LOGGER.debug("Assessment score updated successfully...");

        	  
          }
      }
        LOGGER.debug("DONE");
        return new ExecutionResult<>(MessageResponseFactory.createCreatedResponse(), ExecutionStatus.SUCCESSFUL);
      }   
    
    private static class DefAJEntityRubricGradingEntityBuilder implements EntityBuilder<AJEntityRubricGrading> {
    }
    

    @Override
    public boolean handlerReadOnly() {
        return false;
    }
       

}
