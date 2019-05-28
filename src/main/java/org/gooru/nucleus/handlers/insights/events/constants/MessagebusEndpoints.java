package org.gooru.nucleus.handlers.insights.events.constants;

public final class MessagebusEndpoints {
  /*
   * Any change here in end points should be done in the gateway side as well,
   * as both sender and receiver should be in sync
   */

  // Class Reporting EndPoint
  public static final String MBEP_ANALYTICS_WRITE = "org.gooru.nucleus.message.bus.analytics.write";
  public static final String MBEP_EVENT = "org.gooru.nucleus.message.bus.publisher.event";
  //Teacher Score Override - Update Event
  public static final String MBEP_ANALYTICS_UPDATE = "org.gooru.nucleus.message.bus.analytics.update";
  //Student Self Grading
  public static final String MBEP_ANALYTICS_SELF_GRADING_EXT_ASSESSMENT = "org.gooru.nucleus.message.bus.analytics.self.grade.ext.assessment";
  //Student Offline Report
  public static final String MBEP_ANALYTICS_OFFLINE_REPORT = "org.gooru.nucleus.message.bus.analytics.offline.report";
  //Post Processor (Process Assessment events for Diagnostics)
  public static final String MBEP_POSTPROCESSOR = "org.gooru.nucleus.message.bus.analytics.postprocessor";
  //Offline Activity
  public static final String MBEP_OFFLINE_ACTIVITY = "org.gooru.nucleus.message.bus.analytics.offline.activity";
  

  private MessagebusEndpoints() {
    throw new AssertionError();
  }
}
