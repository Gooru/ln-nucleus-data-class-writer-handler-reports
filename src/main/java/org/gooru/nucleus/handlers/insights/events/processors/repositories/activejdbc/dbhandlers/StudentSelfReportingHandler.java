package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.GEPConstants;
import org.gooru.nucleus.handlers.insights.events.processors.MessageDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;

import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityRubricGrading;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.EntityBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import com.hazelcast.util.StringUtil;


public class StudentSelfReportingHandler implements DBHandler {
	
	  private static final Logger LOGGER = LoggerFactory.getLogger(StudentSelfReportingHandler.class);
	  private static final String USER_ID_FROM_SESSION = "userIdFromSession";
	  private static final String EXT_COLLECTION_ID = "external_collection_id";
	  private static final String USER_ID = "user_id";
	  private static final String PERCENT_SCORE = "percent_score";
	  private static final String SCORE = "score";
	  private static final String MAX_SCORE = "max_score";
	  private static final String EVIDENCE = "evidence";
	  private final ProcessorContext context;
	  private AJEntityReporting baseReports;	  
	  private Double score;
	  private Double percentScore;
	  private Double rawScore;
	  private Double maxScore;
	  String localeDate;

	    public StudentSelfReportingHandler(ProcessorContext context) {	    	
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
	    	  if (!context.request().getString("userIdFromSession").equals(context.request().getString(USER_ID))) {
		           return new
		           ExecutionResult<>(MessageResponseFactory.createForbiddenResponse
		        		   ("Auth Failure"), ExecutionStatus.FAILED);	    		  
	    	  }
	      } else if (StringUtil.isNullOrEmpty(context.request().getString("userIdFromSession"))) {
	           return new
	           ExecutionResult<>(MessageResponseFactory.createForbiddenResponse
	        		   ("Auth Failure"), ExecutionStatus.FAILED);	    		  	    	  
	      }
	      LOGGER.debug("validateRequest() OK");
	      return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
	    }


	    @Override
	      public ExecutionResult<MessageResponse> executeRequest() {
	    
	        baseReports = new AJEntityReporting();
	        JsonObject req = context.request(); 
	        LazyList<AJEntityReporting> duplicateRow = null;
	       	        
	        String studentId = req.getString(USER_ID_FROM_SESSION);
	        req.remove(USER_ID_FROM_SESSION);
	        
	        String extCollectionId = req.getString(EXT_COLLECTION_ID);
	        String userId = req.getString(USER_ID);
	        
	        if (StringUtil.isNullOrEmpty(extCollectionId) || StringUtil.isNullOrEmpty(userId)) {
	            return new ExecutionResult<>(
	                    MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
	                    ExecutionStatus.FAILED);        	
	        }
	        
	        baseReports.set(AJEntityReporting.GOORUUID, userId);
	        baseReports.set(AJEntityReporting.COLLECTION_OID, extCollectionId);	        
	        percentScore = (req.getValue(PERCENT_SCORE) != null) ? Double.valueOf(req.getValue(PERCENT_SCORE).toString()) : null;	        
	        if(percentScore != null) {
	        	int compVal = percentScore.compareTo(100.00);
	        	if (compVal > 0) {
	        		return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Numeric Field Overflow - Invalid Percent Score"), ExecutionResult.ExecutionStatus.FAILED);
	        	} else {
		        	baseReports.set(AJEntityReporting.SCORE, percentScore);
		        	baseReports.set(AJEntityReporting.MAX_SCORE, 100);	        	}
	        } else if (req.getValue(SCORE) == null || req.getValue(SCORE) == null ) {
	        	return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"), ExecutionResult.ExecutionStatus.FAILED);
	        } else {
		        rawScore = Double.valueOf(req.getValue(SCORE).toString());
		        maxScore = Double.valueOf(req.getValue(MAX_SCORE).toString());
		        if (maxScore > 0) {
		        	score = (rawScore *100)/maxScore;		        	
		        	baseReports.set(AJEntityReporting.SCORE, score);
		        	baseReports.set(AJEntityReporting.MAX_SCORE, maxScore);
		        }
	        }
	        
	        //Remove ALL the values from the Request that needed processing, so that the rest of the values from 
	        // the request can be mapped to model
	        req.remove(MAX_SCORE);
	        req.remove(PERCENT_SCORE);
	        req.remove(SCORE);
	        req.remove(EXT_COLLECTION_ID);
	        req.remove(USER_ID);
	        req.remove(AJEntityReporting.COLLECTION_OID);
	        req.remove(EVIDENCE);

	        //& set the fields required at base reports for future reporting
	        baseReports.set(AJEntityReporting.EVENTNAME, EventConstants.COLLECTION_PLAY);
	        baseReports.set(AJEntityReporting.EVENTTYPE, EventConstants.STOP);
	        //baseReports.set(AJEntityReporting.EVENT_ID, UUID.randomUUID());

	    	new DefAJEntityReportingBuilder().build(baseReports, req, AJEntityReporting.getConverterRegistry());
	        if (baseReports.get(AJEntityReporting.COURSE_GOORU_OID) == null || 
	        		baseReports.get(AJEntityReporting.SESSION_ID) == null) {
	        	
	            return new ExecutionResult<>(
	                    MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"),
	                    ExecutionStatus.FAILED);        	
	        }
	        
	        long ts = System.currentTimeMillis();
	        baseReports.set(AJEntityReporting.CREATE_TIMESTAMP,new Timestamp(ts));
	        baseReports.set(AJEntityReporting.UPDATE_TIMESTAMP,new Timestamp(ts));
	        
	        if (baseReports.get(AJEntityReporting.TIME_ZONE) != null) {        	
	          	String timeZone = baseReports.get(AJEntityReporting.TIME_ZONE).toString();	          	
	          	localeDate = UTCToLocale(ts, timeZone);
	          	
	          	if (localeDate != null) {
	              	baseReports.setDateinTZ(localeDate);
	          	}
	          }

	    	duplicateRow =  AJEntityReporting.findBySQL(AJEntityReporting.CHECK_IF_EXT_ASSESSMENT_SELF_GRADED, 
	    			baseReports.get(AJEntityReporting.GOORUUID),
	    			baseReports.get(AJEntityReporting.CLASS_GOORU_OID), baseReports.get(AJEntityReporting.COLLECTION_OID),
	    			baseReports.get(AJEntityRubricGrading.SESSION_ID), EventConstants.COLLECTION_PLAY, EventConstants.STOP);

	    	if (duplicateRow == null || duplicateRow.isEmpty()) {
	    		boolean result = baseReports.save();    		
		    	
	    		if (!result) {
	    			LOGGER.error("ERROR.Student self grades for ext assessments cannot be inserted into the DB: " + req);
	    			if (baseReports.hasErrors()) {
	    				Map<String, String> map = baseReports.errors();
	    				JsonObject errors = new JsonObject();
	    				map.forEach(errors::put);
	    				return new ExecutionResult<>(MessageResponseFactory.createValidationErrorResponse(errors), ExecutionResult.ExecutionStatus.FAILED);
	    			}
	    		}
	    		LOGGER.info("Student Self grades for External Assessments stored successfully " + req);
		        return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(), ExecutionStatus.SUCCESSFUL);
		      } else {
		    	  LOGGER.info("Duplicate record exists. Updating the Self graded score ");
	                duplicateRow.forEach(dup -> {
	                    int id = Integer.valueOf(dup.get("id").toString());
	                    //TODO: Update Timespent, when it is available - The existing TS should be ADDED to the TS available 
	                    //in the current payload
	        	        if(percentScore != null) {
	        	        	Base.exec(AJEntityReporting.UPDATE_SELF_GRADED_EXT_ASSESSMENT, percentScore, 100, new Timestamp(ts), 
	        	        			baseReports.get(AJEntityReporting.TIME_ZONE), baseReports.get(AJEntityReporting.DATE_IN_TIME_ZONE), id);
	        	        } else {
	        	        	Base.exec(AJEntityReporting.UPDATE_SELF_GRADED_EXT_ASSESSMENT, score, maxScore, new Timestamp(ts), 
	        	        			baseReports.get(AJEntityReporting.TIME_ZONE), baseReports.get(AJEntityReporting.DATE_IN_TIME_ZONE), id);	        	        	
	        	        }                                        	  

	                  });
	                
	                LOGGER.info("Student Self grades for External Assessments stored successfully " + req);	                
	                return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(), ExecutionStatus.SUCCESSFUL);
	    		
	    	}
	    }
	        
	    	   
	    
	    private static class DefAJEntityReportingBuilder implements EntityBuilder<AJEntityReporting> {
	    }
	    

	    @Override
	    public boolean handlerReadOnly() {
	        return false;
	    }
	    
	    private String UTCToLocale(Long strUtcDate, String timeZone){
	        
	        String strLocaleDate = null;
	        try{
	            Long epohTime = strUtcDate;
	        	Date utcDate = new Date(epohTime);

	            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	            simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
	            String strUTCDate = simpleDateFormat.format(utcDate);
	            simpleDateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
	            
	            strLocaleDate = simpleDateFormat.format(utcDate);
	            
	            LOGGER.debug("UTC Date String: " + strUTCDate);
	            LOGGER.debug("Locale Date String: " + strLocaleDate);            
	            
	        }catch(Exception e){
	            LOGGER.error(e.getMessage());            
	        }
	        
	        return strLocaleDate;
	    }
	    
}
