package org.gooru.nucleus.handlers.insights.events.rda.processor.resource;

/**
 * @author renuka@gooru
 */
public class ResourceEventConstants {

  public static class EventAttributes {

    public static final String EVENT_NAME = "eventName";
    public static final String EVENT_ID = "eventId";
    public static final String USER_ID = "userId";
    public static final String RESOURCE_ID = "resourceId";
    public static final String RESOURCE_TYPE = "resourceType";
    public static final String CLASS_ID = "classId";
    public static final String COURSE_ID = "courseId";
    public static final String UNIT_ID = "unitId";
    public static final String LESSON_ID = "lessonId";
    public static final String COLLECTION_ID = "collectionId";
    public static final String COLLECTION_TYPE = "collectionType";
    public static final String CONTEXT_COLLECTION_ID = "contextCollectionId";
    public static final String CONTEXT_COLLECTION_TYPE = "contextCollectionType";

    public static final String SESSION_ID = "sessionId";
    public static final String PATH_ID = "pathId";
    public static final String PATH_TYPE = "pathType";

    public static final String PARTNER_ID = "partnerId";
    public static final String TENANT_ID = "tenantId";

    public static final String SCORE = "score";
    public static final String MAX_SCORE = "maxScore";
    public static final String TIMESPENT = "timeSpent";

    public static final String ACTIVITY_TIME = "activityTime";
    public static final String TIMEZONE = "timezone";

    public static final String RESULT = "result";
    public static final String CONTEXT = "context";

    public static final String RESOURCE = "resource";
    public static final String QUESTION = "question";

    public static final String RESOURCE_PERF_EVENT = "rda.resource.performance";

    private EventAttributes() {
      throw new AssertionError();
    }
  }

}
