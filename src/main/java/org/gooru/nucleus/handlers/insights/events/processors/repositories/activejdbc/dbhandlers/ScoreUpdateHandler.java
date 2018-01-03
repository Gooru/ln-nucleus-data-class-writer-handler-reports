package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;

import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.EntityBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;


/**
 * Created by mukul@gooru 
 * 
 */

public class ScoreUpdateHandler implements DBHandler {
	
  private static final Logger LOGGER = LoggerFactory.getLogger(ScoreUpdateHandler.class);
  private static final String USER_ID_FROM_SESSION = "userIdFromSession";
  private static final String STUDENT_ID = "student_id";
  private final ProcessorContext context;
  private AJEntityReporting BaseReports;
  private Double score;
  private Double max_score;

    public ScoreUpdateHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
         if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("Invalid Data");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid Data"),
                ExecutionStatus.FAILED);
        }

        LOGGER.debug("checkSanity() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    public ExecutionResult<MessageResponse> validateRequest() {
      if (context.request().getString("userIdFromSession") != null) {
        List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, context.request().getString("class_id"),
                context.request().getString("userIdFromSession"));
        if (owner.isEmpty()) {
          LOGGER.warn("User is not a teacher or collaborator");
          // return new
          // ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User
          // is not a teacher/collaborator"), ExecutionStatus.FAILED);
        }
      }
      LOGGER.debug("validateRequest() OK");
      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
      public ExecutionResult<MessageResponse> executeRequest() {
    
        BaseReports = new AJEntityReporting();    
        JsonObject req = context.request(); 
        String teacherId = req.getString(USER_ID_FROM_SESSION);
        req.remove(USER_ID_FROM_SESSION);
        String studentId = req.getString(STUDENT_ID);
        req.remove(STUDENT_ID);
        
        new DefAJEntityReportingBuilder().build(BaseReports, req, AJEntityReporting.getConverterRegistry());
        
        //TODO: Move this to a validator function
        if (BaseReports.get(AJEntityReporting.SCORE) == null || BaseReports.get(AJEntityReporting.SESSION_ID) == null  
        		|| BaseReports.get(AJEntityReporting.RESOURCE_ID) == null || BaseReports.get(AJEntityReporting.COLLECTION_OID) == null) {
        	
            return new ExecutionResult<>(
                    MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
                    ExecutionStatus.FAILED);        	
        }

        //TODO: Move this to a validator function
        if ((Double.valueOf(BaseReports.get(AJEntityReporting.SCORE).toString()) == 0.0) && !(BaseReports.get(AJEntityReporting.RESOURCE_ATTEMPT_STATUS).toString().equalsIgnoreCase("incorrect"))    
        		|| (Double.valueOf(BaseReports.get(AJEntityReporting.SCORE).toString()) == 1.0) && !(BaseReports.get(AJEntityReporting.RESOURCE_ATTEMPT_STATUS).toString().equalsIgnoreCase("correct"))) {
        	
            return new ExecutionResult<>(
                    MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
                    ExecutionStatus.FAILED);        	
        }
        
          LOGGER.debug("Student Score : {} ", BaseReports.get(AJEntityReporting.SCORE));
          LOGGER.debug("session id : {} ", BaseReports.get(AJEntityReporting.SESSION_ID));
          LOGGER.debug("resource id : {} ", BaseReports.get(AJEntityReporting.RESOURCE_ID));
          LOGGER.debug("student id : {} ", studentId);
          
          //Since this NOT a Free-Response Question, the Max_Score can be safely assumed to be 1.0. So no need to update the same.
          Base.exec(AJEntityReporting.UPDATE_QUESTION_SCORE_U, BaseReports.get(AJEntityReporting.SCORE),
                  true, BaseReports.get(AJEntityReporting.RESOURCE_ATTEMPT_STATUS), studentId, BaseReports.get(AJEntityReporting.SESSION_ID), BaseReports.get(AJEntityReporting.COLLECTION_OID),
                  BaseReports.get(AJEntityReporting.RESOURCE_ID));
         
        	  LOGGER.debug("Computing total score...");
              LazyList<AJEntityReporting> scoreTS = AJEntityReporting.findBySQL(AJEntityReporting.COMPUTE_ASSESSMENT_SCORE_POST_GRADING_U, 
            		  studentId, BaseReports.get(AJEntityReporting.COLLECTION_OID), BaseReports.get(AJEntityReporting.SESSION_ID));
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
                  LOGGER.debug("Re-Computed total Assessment score {} ", score);
                }
              }
              Base.exec(AJEntityReporting.UPDATE_ASSESSMENT_SCORE_U, score, max_score, studentId, 
            		  BaseReports.get(AJEntityReporting.SESSION_ID), BaseReports.get(AJEntityReporting.COLLECTION_OID));
              LOGGER.debug("Total score updated successfully...");
      
        LOGGER.debug("DONE");
        return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(), ExecutionStatus.SUCCESSFUL);
      }   
    
    private static class DefAJEntityReportingBuilder implements EntityBuilder<AJEntityReporting> {
    }
    

    @Override
    public boolean handlerReadOnly() {
        return false;
    }
      

}
