package org.gooru.nucleus.handlers.insights.events.rda.processor.collection;

/**
 * @author renuka@gooru
 */
public class CollectionEventConstants {

  public static class EventAttributes {

    public static final String EVENT_NAME = "eventName";
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
    public static final String QUESTION_COUNT = "questionCount";
    public static final String PARTNER_ID = "partnerId";
    public static final String TENANT_ID = "tenantId";

    public static final String ACTIVITY_TIME = "activityTime";
    public static final String SCORE = "score";
    public static final String MAX_SCORE = "maxScore";
    public static final String TIMESPENT = "timeSpent";

    public static final String RESULT = "result";
    public static final String CONTEXT = "context";

    public static final String VIEWS = "views";
    public static final String EVENT_TYPE = "eventType";
    public static final String REACTION = "reaction";
    public static final String TIMEZONE = "timezone";
    public static final String IS_GRADED = "isGraded";
    public static final String STATUS = "status";

    public static final String COLLECTION_PERF_EVENT = "rda.collection.performance";
    public static final String COLLECTION_START_EVENT = "rda.collection.start";
    public static final String COLLECTION_SCORE_UPDATE_EVENT = "rda.collection.score.update";
    public static final String COLLECTION_SELF_GRADE_EVENT = "rda.collection.self.grade";
    public static final String OFFLINE_STUDENT_COLLECTION_PERF_EVENT = "rda.collection.offline.student.perf";
    public static final String OFFLINE_ACTIVITY_TEACHER_GRADE_EVENT = "rda.collection.oa.teacher.grade";
    public static final String CONTENT_SOURCE = "contentSource";

    private EventAttributes() {
      throw new AssertionError();
    }
  }

  public static final String COLLECTION = "collection";
  public static final String EXT_COLLECTION = "collection-external";
  public static final String ASSESSMENT = "assessment";
  public static final String EXT_ASSESSMENT = "assessment-external";

}
