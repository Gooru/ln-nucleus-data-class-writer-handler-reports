package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher;

/**
 * @author renuka
 */

final class CoreContentSourceFinder {
  private static final String CORE_COURSE_MAP = "course-map";
  private static final String CORE_CLASS_ACTIVITY = "class-activity";
  private static final String COURSE_MAP = "coursemap";
  private static final String DAILY_CLASS_ACTIVITY = "dailyclassactivity";

  private CoreContentSourceFinder() {
    throw new AssertionError();
  }

  static String findContentSource(String contentSource) {
    if (contentSource != null) {
      if (contentSource.equalsIgnoreCase(COURSE_MAP)) {
        return CORE_COURSE_MAP;
      } else if (contentSource.equalsIgnoreCase(DAILY_CLASS_ACTIVITY)) {
        return CORE_CLASS_ACTIVITY;
      }
    }
    return contentSource;
  }
}
