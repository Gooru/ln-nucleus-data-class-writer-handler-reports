package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.GEPEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher.RDAEventDispatcher;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.EntityBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.utils.BaseUtil;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author renuka@gooru
 * 
 */
public class OfflineStudentReportingHandler implements DBHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(OfflineStudentReportingHandler.class);
    private static final String USER_ID_FROM_SESSION = "userIdFromSession";
    private static final String STUDENT_ID = "student_id";
    private static final String PERCENT_SCORE = "percent_score";
    private static final String EVIDENCE = "evidence";
    private static final String RESOURCES = "resources";
    private static final String ANSWER_STATUS = "answer_status";
    private final ProcessorContext context;
    private AJEntityReporting baseReports;
    private Double finalScore;
    private Double finalMaxScore;
    private Integer questionCount;
    private String localeDate;
    private Long views = 1L;
    private Long reaction = 0L;
    private Long totalResTS = 0L;
    private Double totalResScore;
    private Double totalResMaxScore;
    private Boolean isGraded;
    private String userId;
    private JsonArray userIds;
    
    public OfflineStudentReportingHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("Invalid Data");
            return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid Data"), ExecutionStatus.FAILED);
        }

        try {
            userId = context.request().getString(STUDENT_ID);
        } catch (Exception e) {
            userIds = context.request().getJsonArray(STUDENT_ID);
        }
        
        if (StringUtil.isNullOrEmpty(context.request().getString(AJEntityReporting.COLLECTION_OID)) || !(StringUtils.isNotEmpty(userId) || userIds != null) ||
            StringUtil.isNullOrEmpty(context.request().getString(AJEntityReporting.COURSE_GOORU_OID)) || StringUtil.isNullOrEmpty(context.request().getString(AJEntityReporting.SESSION_ID))) {
            LOGGER.warn("Invalid Json Payload");
            return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"), ExecutionStatus.FAILED);
        }
        LOGGER.debug("checkSanity() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ExecutionResult<MessageResponse> validateRequest() {
        if (context.request().getString("userIdFromSession") != null) {
            List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER, context.request().getString("class_id"), context.request().getString("userIdFromSession"));
            if (owner.isEmpty()) {
                LOGGER.warn("User is not a teacher or collaborator");
                return new ExecutionResult<>(MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"), ExecutionStatus.FAILED);
            }
        }
        LOGGER.debug("validateRequest() OK");
        return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
    }

    @Override
    public ExecutionResult<MessageResponse> executeRequest() {

        baseReports = new AJEntityReporting();
        LazyList<AJEntityReporting> duplicateRow = null;
        JsonObject requestPayload = context.request();
        String collectionId = requestPayload.getString(AJEntityReporting.COLLECTION_OID);
        String collectionType = requestPayload.getString(AJEntityReporting.COLLECTION_TYPE);
        requestPayload.remove(USER_ID_FROM_SESSION);
        requestPayload.remove(STUDENT_ID);

        long ts = System.currentTimeMillis();
        if (requestPayload.getString(AJEntityReporting.TIME_ZONE) != null) {
            String timeZone = requestPayload.getString(AJEntityReporting.TIME_ZONE);
            localeDate = BaseUtil.UTCToLocale(ts, timeZone);
            if (localeDate != null) {
                baseReports.setDateinTZ(localeDate);
            }
        }
        
        if (requestPayload.containsKey(AJEntityReporting.TIMESPENT) && requestPayload.getLong(AJEntityReporting.TIMESPENT) != null) this.totalResTS = requestPayload.getLong(AJEntityReporting.TIMESPENT);
        if (requestPayload.containsKey(AJEntityReporting.REACTION) && requestPayload.getLong(AJEntityReporting.REACTION) != null) this.reaction = requestPayload.getLong(AJEntityReporting.REACTION);
        if (requestPayload.containsKey(AJEntityReporting.SCORE) && requestPayload.getDouble(AJEntityReporting.SCORE) != null) this.finalScore = requestPayload.getDouble(AJEntityReporting.SCORE);
        if (requestPayload.containsKey(AJEntityReporting.MAX_SCORE) && requestPayload.getDouble(AJEntityReporting.MAX_SCORE) != null) this.finalMaxScore = requestPayload.getDouble(AJEntityReporting.MAX_SCORE);
        if (requestPayload.containsKey(AJEntityReporting.QUESTION_COUNT) && requestPayload.getInteger(AJEntityReporting.QUESTION_COUNT) != null) this.questionCount = requestPayload.getInteger(AJEntityReporting.QUESTION_COUNT);

        // Generate and store resource play events
        processResourcePlayData(requestPayload, collectionId, userId, ts);

        
        if (collectionType.equalsIgnoreCase(EventConstants.EXTERNAL_ASSESSMENT) || collectionType.equalsIgnoreCase(EventConstants.EXTERNAL_COLLECTION)) {
            Double percentScore = (requestPayload.getValue(PERCENT_SCORE) != null) ? Double.valueOf(requestPayload.getValue(PERCENT_SCORE).toString()) : null;
            if (percentScore != null) {
                if ((percentScore.compareTo(100.00) > 0) || (percentScore.compareTo(0.00) < 0)) {
                    return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Numeric Field Overflow - Invalid Percent Score"), ExecutionResult.ExecutionStatus.FAILED);
                } else {
                    this.finalScore = percentScore;
                    this.finalMaxScore = 100.0;
                }
                this.isGraded = true;
            } else if ((collectionType.equalsIgnoreCase(EventConstants.EXTERNAL_ASSESSMENT)) && (requestPayload.getValue(AJEntityReporting.SCORE) == null || requestPayload.getValue(AJEntityReporting.MAX_SCORE) == null)) {
                return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid Json Payload"), ExecutionResult.ExecutionStatus.FAILED);
            } else if (requestPayload.getValue(AJEntityReporting.SCORE) != null && requestPayload.getValue(AJEntityReporting.MAX_SCORE) != null) {
                Double rawScore = Double.valueOf(requestPayload.getValue(AJEntityReporting.SCORE).toString());
                Double maxScore = Double.valueOf(requestPayload.getValue(AJEntityReporting.MAX_SCORE).toString());
                Double score = null;
                if ((rawScore.compareTo(100.00) > 0) || (maxScore.compareTo(100.00) > 0) || (rawScore.compareTo(0.00) < 0) || (maxScore.compareTo(0.00) < 0) || (maxScore.compareTo(0.00) == 0)) {
                    return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Numeric Field Overflow - Invalid Fraction Score"), ExecutionResult.ExecutionStatus.FAILED);
                }
                score = (rawScore * 100) / maxScore;
                this.finalScore = score;
                this.finalMaxScore = maxScore;
                this.isGraded = true;
            }
        } else if (this.totalResScore != null && this.totalResMaxScore != null && this.totalResMaxScore > 0.0) {
            //if resource play data is given in request, then use total score of questions
            finalScore = ((this.totalResScore * 100) / this.totalResMaxScore);
            finalMaxScore = this.totalResMaxScore;
            LOGGER.debug("Re-Computed total Assessment score {} ", finalScore);
        }        
        
        baseReports.set(AJEntityReporting.IS_GRADED, this.isGraded);
        baseReports.set(AJEntityReporting.VIEWS, this.views);
        baseReports.set(AJEntityReporting.TIMESPENT, this.totalResTS);
        baseReports.set(AJEntityReporting.REACTION, this.reaction);
        baseReports.set(AJEntityReporting.SCORE, this.finalScore);
        baseReports.set(AJEntityReporting.MAX_SCORE, this.finalMaxScore);
        baseReports.set(AJEntityReporting.QUESTION_COUNT, this.questionCount);
        baseReports.set(AJEntityReporting.GOORUUID, userId);
        baseReports.set(AJEntityReporting.COLLECTION_OID, collectionId);
        baseReports.set(AJEntityReporting.RESOURCE_TYPE, EventConstants.NA);
        baseReports.set(AJEntityReporting.QUESTION_TYPE, EventConstants.NA);
        baseReports.set(AJEntityReporting.EVENTNAME, EventConstants.COLLECTION_PLAY);
        baseReports.set(AJEntityReporting.EVENTTYPE, EventConstants.STOP);
        baseReports.set(AJEntityReporting.CREATE_TIMESTAMP, new Timestamp(ts));
        baseReports.set(AJEntityReporting.UPDATE_TIMESTAMP, new Timestamp(ts));
        
        //Remove ALL the values from the Request that needed processing, so that the rest of the values from the request can be mapped to model
        removeProcessedFieldsFromPayload(requestPayload);
        new DefAJEntityReportingBuilder().build(baseReports, requestPayload, AJEntityReporting.getConverterRegistry());
        
        duplicateRow =
            AJEntityReporting.findBySQL(AJEntityReporting.CHECK_DUPLICATE_COLLECTION_EVENT, userId, 
                baseReports.get(AJEntityReporting.SESSION_ID), baseReports.get(AJEntityReporting.COLLECTION_OID), EventConstants.STOP, EventConstants.COLLECTION_PLAY);

        if (baseReports.hasErrors()) {
            LOGGER.warn("errors in creating Base Report");            
        }
        LOGGER.debug("OSRH::Inserting CP event into Reports DB: " + context.request().toString());
        
        if (baseReports.isValid()) {
            if (duplicateRow == null || duplicateRow.isEmpty()) {
              //Set timespent for all students of the class
                if (collectionType.equalsIgnoreCase(EventConstants.EXTERNAL_COLLECTION) && userIds != null && !userIds.isEmpty()) {
                    userIds.forEach(user -> {
                        AJEntityReporting baseReports = new AJEntityReporting();
                        this.baseReports.toMap().keySet().forEach(key -> {
                            baseReports.set(key, this.baseReports.get(key));
                        });
                        baseReports.set(AJEntityReporting.GOORUUID, user.toString());
                        if (baseReports.insert()) {
                            LOGGER.info("Offline student record (Ext-Coll) inserted successfully into Reports DB");
                            sendCPEventToGEPAndRDA(baseReports, ts);
                        } else {
                            LOGGER.error("Error while inserting offline student event into Reports DB: " + context.request().toString());
                        }
                    });
                } else if (baseReports.insert()) {
                    LOGGER.info("Offline student record inserted successfully into Reports DB");
                    sendCPEventToGEPAndRDA(baseReports, ts);
                } else {
                    LOGGER.error("Error while inserting offline student event into Reports DB: " + context.request().toString());
                }
            } else {
                LOGGER.info("Duplicate record exists. Ignoring offline student score ");
            }
        } else {
            LOGGER.warn("Event validation error");
            return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(), ExecutionStatus.FAILED);
        }
        return new ExecutionResult<>(MessageResponseFactory.createOkayResponse(), ExecutionStatus.SUCCESSFUL);
        
    }

    private void sendCPEventToGEPAndRDA(AJEntityReporting baseReports, long ts) {
        RDAEventDispatcher rdaEventDispatcher = new RDAEventDispatcher(baseReports, this.views, this.reaction, this.totalResTS, this.finalMaxScore, this.finalScore, this.isGraded, ts);
        rdaEventDispatcher.sendOfflineStudentReportEventToRDA();
        GEPEventDispatcher eventDispatcher = new GEPEventDispatcher(baseReports, this.totalResTS,this.totalResMaxScore, this.totalResScore, System.currentTimeMillis());
        eventDispatcher.sendCPEventFromBaseReportstoGEP();
    }
    
    private void removeProcessedFieldsFromPayload(JsonObject requestPayload) {
        requestPayload.remove(AJEntityReporting.MAX_SCORE);
        requestPayload.remove(PERCENT_SCORE);
        requestPayload.remove(AJEntityReporting.SCORE);
        requestPayload.remove(STUDENT_ID);
        requestPayload.remove(AJEntityReporting.COLLECTION_OID);
        requestPayload.remove(EVIDENCE);
    }

    private ExecutionResult<MessageResponse> processResourcePlayData(JsonObject requestPayload, String collectionId, String userId, long ts) {
        if (requestPayload.getJsonArray(RESOURCES) != null && !requestPayload.getJsonArray(RESOURCES).isEmpty()) {
            LOGGER.info("OSRH::Processing CRP events..");
            LazyList<AJEntityReporting> duplicateRow;
            JsonArray resources = requestPayload.getJsonArray(RESOURCES);
            requestPayload.remove(RESOURCES);
            this.questionCount = 0;
            this.totalResMaxScore = 0.0;
            this.totalResScore = null;
            for (Object res : resources) {
                AJEntityReporting baseReport = new AJEntityReporting();
                JsonObject resource = (JsonObject) res;
                duplicateRow =
                    AJEntityReporting.findBySQL(AJEntityReporting.FIND_RESOURCE_EVENT, userId, collectionId, requestPayload.getString(AJEntityReporting.SESSION_ID), resource.getString(AJEntityReporting.RESOURCE_ID), EventConstants.STOP);
                
                baseReport.set(AJEntityReporting.RESOURCE_ID, resource.getString(AJEntityReporting.RESOURCE_ID));
                baseReport.set(AJEntityReporting.GOORUUID, userId);
                baseReport.set(AJEntityReporting.COLLECTION_OID, collectionId);
                baseReport.set(AJEntityReporting.EVENTNAME, EventConstants.COLLECTION_RESOURCE_PLAY);
                baseReport.set(AJEntityReporting.EVENTTYPE, EventConstants.STOP);

                baseReport.set(AJEntityReporting.CREATE_TIMESTAMP, new Timestamp(ts));
                baseReport.set(AJEntityReporting.UPDATE_TIMESTAMP, new Timestamp(ts));

                if (localeDate != null) {
                    baseReport.setDateinTZ(localeDate);
                }
                
                String resourceType = resource.getString(AJEntityReporting.RESOURCE_TYPE);
                if (resourceType.equalsIgnoreCase(EventConstants.QUESTION)) {
                    this.questionCount += 1;
                    Double score = resource.getDouble(AJEntityReporting.SCORE);
                    if (score != null) {
                        String answerStatus = EventConstants.ATTEMPTED;
                        if (score == 0) {
                            answerStatus = EventConstants.INCORRECT;
                        } else if (score == 1) {
                            answerStatus = EventConstants.CORRECT;
                        }

                        Double totalResMaxScore = resource.getDouble(AJEntityReporting.MAX_SCORE);
                        if (totalResMaxScore != null) {
                            if (this.totalResScore != null) {
                                this.totalResScore += score;
                            } else {
                                this.totalResScore = score;
                            }

                            if (this.totalResMaxScore != null) {
                                this.totalResMaxScore += totalResMaxScore;
                            } else {
                                this.totalResMaxScore = totalResMaxScore;
                            }
                        }
                        baseReport.set(AJEntityReporting.RESOURCE_ATTEMPT_STATUS, answerStatus);
                        this.isGraded = true;
                        baseReport.setBoolean(AJEntityReporting.IS_GRADED, this.isGraded);
                    }
                }
                
                long views = 1;
                int reaction = resource.containsKey(AJEntityReporting.REACTION) ? resource.getInteger(AJEntityReporting.REACTION) : 0;
                this.totalResTS += resource.getLong(AJEntityReporting.TIMESPENT);
                
                baseReport.set(AJEntityReporting.VIEWS, views);
                baseReport.set(AJEntityReporting.REACTION, reaction);
                baseReport.set(AJEntityReporting.GRADING_TYPE, EventConstants.TEACHER);
                
                resource.remove(ANSWER_STATUS);
                new DefAJEntityReportingBuilder().build(baseReport, requestPayload, AJEntityReporting.getConverterRegistry());
                new DefAJEntityReportingBuilder().build(baseReport, resource, AJEntityReporting.getConverterRegistry());

                if (baseReport.hasErrors()) {
                    LOGGER.warn("errors in creating Base Report");
                }
                LOGGER.debug("Inserting CRP into Reports DB: " + resource.toString());

                if (baseReport.isValid()) {
                    if (duplicateRow == null || duplicateRow.isEmpty()) {
                        if (baseReport.insert()) {
                            LOGGER.info("Offline Student CRP event inserted successfully in Reports DB");
                            GEPEventDispatcher eventDispatcher = new GEPEventDispatcher(baseReport, null, null, null, System.currentTimeMillis());
                            eventDispatcher.sendCRPEventFromBaseReportstoGEP();
                        } else {
                            LOGGER.error("Error while inserting offline student CRP event into Reports DB: " + context.request().toString());
                        }
                    } else {
                        LOGGER.debug("Found duplicate row in DB, so ignoring duplicate row.....");
                    }
                } else {
                    LOGGER.warn("Event validation error");
                    return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(), ExecutionStatus.FAILED);
                }
            }
        }
        return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(), ExecutionStatus.CONTINUE_PROCESSING);
    }

    private static class DefAJEntityReportingBuilder implements EntityBuilder<AJEntityReporting> {
    }
    
    @Override
    public boolean handlerReadOnly() {
        return false;
    }

}
