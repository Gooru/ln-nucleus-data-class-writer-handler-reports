package org.gooru.nucleus.handlers.insights.events.app.components;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This is repository for verticles which needs to be deployed by {@link DeployVerticle}
 *
 * @author Insights Team
 */
public class VerticleRegistry implements Iterable<String> {

  private static final String WRITER_CONSUMER_VERTICLE = "org.gooru.nucleus.handlers.insights.events.bootstrap.MessageConsumerVerticle";
  private static final String EVENT_UPDATE_VERTICLE = "org.gooru.nucleus.handlers.insights.events.bootstrap.EventUpdateVerticle";
  private static final String STUDENT_SELF_GRADE_EXT_ASSESSMENT_VERTICLE = "org.gooru.nucleus.handlers.insights.events.bootstrap.SelfReportingVerticle";
  private static final String RDA_WRITER_CONSUMER_VERTICLE = "org.gooru.nucleus.handlers.insights.events.bootstrap.RDAMessageConsumerVerticle";
  private static final String STUDENT_OFFLINE_REPORT_VERTICLE = "org.gooru.nucleus.handlers.insights.events.bootstrap.OfflineStudentReportingVerticle";
  private static final String POST_PROCESSING_VERTICLE = "org.gooru.nucleus.handlers.insights.events.bootstrap.PostProcessingVerticle";
  private static final String EVENT_BUS_SEND_VERTICLE = "org.gooru.nucleus.handlers.insights.events.bootstrap.EBSendVerticle";
  private static final String OA_VERTICLE = "org.gooru.nucleus.handlers.insights.events.bootstrap.OAVerticle";

  private final Iterator<String> internalIterator;

  public VerticleRegistry() {
    List<String> initializers = new ArrayList<>();
    initializers.add(WRITER_CONSUMER_VERTICLE);
    initializers.add(EVENT_UPDATE_VERTICLE);
    initializers.add(STUDENT_SELF_GRADE_EXT_ASSESSMENT_VERTICLE);
    initializers.add(RDA_WRITER_CONSUMER_VERTICLE);
    initializers.add(STUDENT_OFFLINE_REPORT_VERTICLE);
    initializers.add(POST_PROCESSING_VERTICLE);
    initializers.add(EVENT_BUS_SEND_VERTICLE);
    initializers.add(OA_VERTICLE);
    internalIterator = initializers.iterator();
  }

  @Override
  public Iterator<String> iterator() {
    return new Iterator<String>() {

      @Override
      public boolean hasNext() {
        return internalIterator.hasNext();
      }

      @Override
      public String next() {
        return internalIterator.next();
      }

    };
  }

}
