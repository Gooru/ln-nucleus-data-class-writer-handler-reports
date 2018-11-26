package org.gooru.nucleus.handlers.insights.events.rda.processor.resource;

import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

/**
 * @author renuka@gooru
 * 
 */
public class ResourceEventParser {

    // TODO: Update this Object based on the incoming event
    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceEventParser.class);
    private JsonObject event;

    public ResourceEventParser(String json) throws Exception {
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
    private String sessionId;
    private JsonObject result;
    private JsonObject context;
    private String resourceId;
    private String resourceType;
    private String eventName;
    private String tenantId;
    private String partnerId;
    private long activityTime;
    private double score;
    private double maxScore;
    private long timeSpent;
    private int pathId;
    private String pathType;
    private String timezone;

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

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public long getActivityTime() {
        return activityTime;
    }

    public void setActivityTime(long activityTime) {
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

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public String getResourceType() {
        return resourceType;
    }

    public void setResourceType(String resourceType) {
        this.resourceType = resourceType;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getPartnerId() {
        return partnerId;
    }

    public void setPartnerId(String partnerId) {
        this.partnerId = partnerId;
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

    public String getTimeZone() {
        return timezone;
    }

    public void setTimeZone(String timezone) {
        this.timezone = timezone;
    }

    private ResourceEventParser parse() {
        try {
            this.user = this.event.getString(ResourceEventConstants.EventAttributes.USER_ID);
            this.activityTime = this.event.getLong(ResourceEventConstants.EventAttributes.ACTIVITY_TIME);
            this.resourceId = this.event.getString(ResourceEventConstants.EventAttributes.RESOURCE_ID);
            this.resourceType = this.event.getString(ResourceEventConstants.EventAttributes.RESOURCE_TYPE);
            this.eventName = this.event.getString(ResourceEventConstants.EventAttributes.EVENT_NAME);
            this.timezone = this.event.containsKey(ResourceEventConstants.EventAttributes.TIMEZONE) ? event.getString(ResourceEventConstants.EventAttributes.TIMEZONE) : null;

            this.result = this.event.getJsonObject(ResourceEventConstants.EventAttributes.RESULT);
            if (this.result == null) {
                this.result = new JsonObject();
            }
            this.score = result.containsKey(ResourceEventConstants.EventAttributes.SCORE) ? this.result.getDouble(ResourceEventConstants.EventAttributes.SCORE) : 0;
            this.maxScore = result.containsKey(ResourceEventConstants.EventAttributes.MAX_SCORE) ? this.result.getDouble(ResourceEventConstants.EventAttributes.MAX_SCORE) : 0.0;
            this.timeSpent = result.containsKey(ResourceEventConstants.EventAttributes.TIMESPENT) ? this.result.getLong(ResourceEventConstants.EventAttributes.TIMESPENT) : 0L;

            this.context = this.event.getJsonObject(ResourceEventConstants.EventAttributes.CONTEXT);
            if (this.context == null) {
                this.context = new JsonObject();
            }
            this.classId = context.containsKey(ResourceEventConstants.EventAttributes.CLASS_ID) ? this.context.getString(ResourceEventConstants.EventAttributes.CLASS_ID) : null;
            this.courseId = context.containsKey(ResourceEventConstants.EventAttributes.COURSE_ID) ? this.context.getString(ResourceEventConstants.EventAttributes.COURSE_ID) : null;
            this.unitId = context.containsKey(ResourceEventConstants.EventAttributes.UNIT_ID) ? this.context.getString(ResourceEventConstants.EventAttributes.UNIT_ID) : null;
            this.lessonId = context.containsKey(ResourceEventConstants.EventAttributes.LESSON_ID) ? this.context.getString(ResourceEventConstants.EventAttributes.LESSON_ID) : null;
            this.sessionId = context.containsKey(ResourceEventConstants.EventAttributes.SESSION_ID) ? this.context.getString(ResourceEventConstants.EventAttributes.SESSION_ID) : EventConstants.NA;
            this.partnerId = context.containsKey(ResourceEventConstants.EventAttributes.PARTNER_ID) ? this.context.getString(ResourceEventConstants.EventAttributes.PARTNER_ID) : null;
            this.tenantId = context.containsKey(ResourceEventConstants.EventAttributes.TENANT_ID) ? this.context.getString(ResourceEventConstants.EventAttributes.TENANT_ID) : null;

            this.collectionId = context.containsKey(ResourceEventConstants.EventAttributes.COLLECTION_ID) ? this.context.getString(ResourceEventConstants.EventAttributes.COLLECTION_ID) : null;
            this.collectionType = context.containsKey(ResourceEventConstants.EventAttributes.COLLECTION_TYPE) ? this.context.getString(ResourceEventConstants.EventAttributes.COLLECTION_TYPE) : null;
            this.pathType = context.containsKey(ResourceEventConstants.EventAttributes.PATH_TYPE) ? context.getString(ResourceEventConstants.EventAttributes.PATH_TYPE) : null;
            this.pathId = context.containsKey(ResourceEventConstants.EventAttributes.PATH_ID) && context.getValue(ResourceEventConstants.EventAttributes.PATH_ID) != null ? this.context.getInteger(ResourceEventConstants.EventAttributes.PATH_ID) : 0;

        } catch (Exception e) {
            LOGGER.error("Error in event parser : {}", e);
        }
        return this;
    }

}