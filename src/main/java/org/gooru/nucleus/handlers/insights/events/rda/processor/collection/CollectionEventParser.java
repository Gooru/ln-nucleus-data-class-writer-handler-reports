package org.gooru.nucleus.handlers.insights.events.rda.processor.collection;

import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

/**
 * @author renuka@gooru
 * 
 */
public class CollectionEventParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(CollectionEventParser.class);
    private JsonObject event;

    public CollectionEventParser(String json) throws Exception {
        this.event = new JsonObject(json);
        this.parse();
    }

    private String user;
    private String classId;
    private String courseId;
    private String unitId;
    private String lessonId;
    private String collectionId;
    private String collectionType;
    private String contextCollectionId;
    private String contextCollectionType;
    private String sessionId;
    private JsonObject result;
    private JsonObject context;
    private String eventName;
    private int questionCount;
    private String partnerId;
    private String tenantId;
    private String eventType;
    private String timezone;
    private Long activityTime;
    private double score;
    private double maxScore;
    private long views;
    private int reaction;
    private long timeSpent;
    private int pathId;
    private String pathType;
    private String contentSource;
    private Boolean isGraded;
    private Boolean status;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getClassId() {
        return classId;
    }

    public void setClassId(String classId) {
        this.classId = classId;
    }

    public String getCourseId() {
        return courseId;
    }

    public void setCourseId(String courseId) {
        this.courseId = courseId;
    }

    public String getUnitId() {
        return unitId;
    }

    public void setUnitId(String unitId) {
        this.unitId = unitId;
    }

    public String getLessonId() {
        return lessonId;
    }

    public void setLessonId(String lessonId) {
        this.lessonId = lessonId;
    }

    public String getCollectionId() {
        return collectionId;
    }

    public void setCollectionId(String collectionId) {
        this.collectionId = collectionId;
    }

    public String getCollectionType() {
        return collectionType;
    }

    public void setCollectionType(String collectionType) {
        this.collectionType = collectionType;
    }

    public String getContextCollectionId() {
        return contextCollectionId;
    }

    public void setContextCollectionId(String contextCollectionId) {
        this.contextCollectionId = contextCollectionId;
    }

    public String getContextCollectionType() {
        return contextCollectionType;
    }

    public void setContextCollectionType(String contextCollectionType) {
        this.contextCollectionType = contextCollectionType;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public int getPathId() {
        return pathId;
    }

    public void setPathId(int pathId) {
        this.pathId = pathId;
    }

    public String getPathType() {
        return pathType;
    }

    public void setPathType(String pathType) {
        this.pathType = pathType;
    }

    public Long getActivityTime() {
        return activityTime;
    }

    public void setActivityTime(Long activityTime) {
        this.activityTime = activityTime;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public double getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(double maxScore) {
        this.maxScore = maxScore;
    }

    public long getTimeSpent() {
        return timeSpent;
    }

    public void setTimeSpent(long timeSpent) {
        this.timeSpent = timeSpent;
    }

    public JsonObject getResult() {
        return result;
    }

    public void setResult(JsonObject result) {
        this.result = result;
    }

    public JsonObject getContext() {
        return context;
    }

    public void setContext(JsonObject context) {
        this.context = context;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public int getQuestionCount() {
        return questionCount;
    }

    public void setQuestionCount(int questionCount) {
        this.questionCount = questionCount;
    }

    public String getPartnerId() {
        return partnerId;
    }

    public void setPartnerId(String partnerId) {
        this.partnerId = partnerId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public long getTimespent() {
        return timeSpent;
    }

    public void setTimespent(long timeSpent) {
        this.timeSpent = timeSpent;
    }

    public long getViews() {
        return views;
    }

    public void setViews(long views) {
        this.views = views;
    }

    public long getReaction() {
        return reaction;
    }

    public void setReaction(int reaction) {
        this.reaction = reaction;
    }

    public String getTimeZone() {
        return timezone;
    }

    public void setTimeZone(String timezone) {
        this.timezone = timezone;
    }

    public Boolean getIsGraded() {
        return isGraded;
    }

    public void setIsGraded(Boolean isGraded) {
        this.isGraded = isGraded;
    }

    public String getContentSource() {
        return contentSource;
    }

    public void setContentSource(String contentSource) {
        this.contentSource = contentSource;
    }
    
    public Boolean getStatus() {
        return status;
    }

    public void setStatus(Boolean status) {
        this.status = status;
    }

    private CollectionEventParser parse() {
        try {
            this.user = this.event.getString(CollectionEventConstants.EventAttributes.USER_ID);
            this.activityTime = this.event.getLong(CollectionEventConstants.EventAttributes.ACTIVITY_TIME);
            this.eventName = this.event.getString(CollectionEventConstants.EventAttributes.EVENT_NAME);
            this.collectionId = this.event.getString(CollectionEventConstants.EventAttributes.COLLECTION_ID);
            this.collectionType = this.event.getString(CollectionEventConstants.EventAttributes.COLLECTION_TYPE);
            this.contextCollectionId =
                this.event.containsKey(CollectionEventConstants.EventAttributes.CONTEXT_COLLECTION_ID) ? event.getString(CollectionEventConstants.EventAttributes.CONTEXT_COLLECTION_ID) : null;
            this.contextCollectionType =
                this.event.containsKey(CollectionEventConstants.EventAttributes.CONTEXT_COLLECTION_TYPE) ? event.getString(CollectionEventConstants.EventAttributes.CONTEXT_COLLECTION_TYPE) : null;
            this.timezone = this.event.containsKey(CollectionEventConstants.EventAttributes.TIMEZONE) ? event.getString(CollectionEventConstants.EventAttributes.TIMEZONE) : null;
            this.reaction = this.event.containsKey(CollectionEventConstants.EventAttributes.REACTION) ? event.getInteger(CollectionEventConstants.EventAttributes.REACTION) : 0;

            this.result = this.event.getJsonObject(CollectionEventConstants.EventAttributes.RESULT);
            if (this.result == null) {
                this.result = new JsonObject();
            }
            this.score = this.result.containsKey(CollectionEventConstants.EventAttributes.SCORE) ? this.result.getDouble(CollectionEventConstants.EventAttributes.SCORE) : 0;
            this.maxScore = this.result.containsKey(CollectionEventConstants.EventAttributes.MAX_SCORE) ? this.result.getDouble(CollectionEventConstants.EventAttributes.MAX_SCORE) : 0.0;
            this.timeSpent = this.result.containsKey(CollectionEventConstants.EventAttributes.TIMESPENT) ? this.result.getLong(CollectionEventConstants.EventAttributes.TIMESPENT) : 0L;
            this.views = this.result.containsKey(CollectionEventConstants.EventAttributes.VIEWS) ? this.result.getLong(CollectionEventConstants.EventAttributes.VIEWS) : 0L;
            this.isGraded = this.result.containsKey(CollectionEventConstants.EventAttributes.IS_GRADED) ? this.result.getBoolean(CollectionEventConstants.EventAttributes.IS_GRADED) : null;
            this.status = this.result.containsKey(CollectionEventConstants.EventAttributes.STATUS) ? this.result.getBoolean(CollectionEventConstants.EventAttributes.STATUS) : false;

            this.context = this.event.getJsonObject(CollectionEventConstants.EventAttributes.CONTEXT);
            if (this.context == null) {
                this.context = new JsonObject();
            }
            this.eventType =
                this.context.containsKey(CollectionEventConstants.EventAttributes.EVENT_TYPE) ? this.context.getString(CollectionEventConstants.EventAttributes.EVENT_TYPE) : EventConstants.NA;
            this.classId = this.context.containsKey(CollectionEventConstants.EventAttributes.CLASS_ID) ? this.context.getString(CollectionEventConstants.EventAttributes.CLASS_ID) : null;
            this.courseId = this.context.containsKey(CollectionEventConstants.EventAttributes.COURSE_ID) ? this.context.getString(CollectionEventConstants.EventAttributes.COURSE_ID) : null;
            this.unitId = this.context.containsKey(CollectionEventConstants.EventAttributes.UNIT_ID) ? this.context.getString(CollectionEventConstants.EventAttributes.UNIT_ID) : null;
            this.lessonId = this.context.containsKey(CollectionEventConstants.EventAttributes.LESSON_ID) ? this.context.getString(CollectionEventConstants.EventAttributes.LESSON_ID) : null;
            this.sessionId =
                this.context.containsKey(CollectionEventConstants.EventAttributes.SESSION_ID) ? this.context.getString(CollectionEventConstants.EventAttributes.SESSION_ID) : EventConstants.NA;
            this.partnerId = this.context.containsKey(CollectionEventConstants.EventAttributes.PARTNER_ID) ? this.context.getString(CollectionEventConstants.EventAttributes.PARTNER_ID) : null;
            this.tenantId = this.context.containsKey(CollectionEventConstants.EventAttributes.TENANT_ID) ? this.context.getString(CollectionEventConstants.EventAttributes.TENANT_ID) : null;
            this.pathType = this.context.containsKey(CollectionEventConstants.EventAttributes.PATH_TYPE) ? this.context.getString(CollectionEventConstants.EventAttributes.PATH_TYPE) : null;
            this.pathId = this.context.containsKey(CollectionEventConstants.EventAttributes.PATH_ID) ? this.context.getInteger(CollectionEventConstants.EventAttributes.PATH_ID) : 0;
            this.contentSource = this.context.containsKey(CollectionEventConstants.EventAttributes.CONTENT_SOURCE) ? this.context.getString(CollectionEventConstants.EventAttributes.CONTENT_SOURCE) : null;
            this.questionCount =
                this.context.containsKey(CollectionEventConstants.EventAttributes.QUESTION_COUNT) ? this.context.getInteger(CollectionEventConstants.EventAttributes.QUESTION_COUNT) : 0;

        } catch (Exception e) {
            LOGGER.error("Error in event parser : {}", e);
        }
        return this;

    }

}
