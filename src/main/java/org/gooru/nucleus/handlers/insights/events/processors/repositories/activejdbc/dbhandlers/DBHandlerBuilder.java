package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.RDAProcessorContext;
import org.gooru.nucleus.handlers.insights.events.rda.processor.collection.CollectionScoreUpdateEventRDAHandler;
import org.gooru.nucleus.handlers.insights.events.rda.processor.collection.CollectionStartEventRDAHandler;
import org.gooru.nucleus.handlers.insights.events.rda.processor.collection.CollectionStopEventRDAHandler;
import org.gooru.nucleus.handlers.insights.events.rda.processor.collection.OATeacherGradeEventRDAHandler;
import org.gooru.nucleus.handlers.insights.events.rda.processor.collection.OfflineStudentPerfEventRDAHandler;
import org.gooru.nucleus.handlers.insights.events.rda.processor.collection.StudentSelfGradeEventRDAHandler;
import org.gooru.nucleus.handlers.insights.events.rda.processor.resource.ResourceStopEventRDAHandler;

/**
 * Created by mukul@gooru
 */
public final class DBHandlerBuilder {

  private DBHandlerBuilder() {
    throw new AssertionError();
  }

  public static DBHandler buildProcessEventHandler(ProcessorContext context) {
    return new ProcessEventHandler(context);
  }

  public static DBHandler buildCompetencyReportsHandler(ProcessorContext context) {
    return new ProcessCompetencyReportHandler(context);
  }

  public static DBHandler buildDailyClassActivityEventHandler(ProcessorContext context) {
    return new DailyClassActivityEventHandler(context);
  }

  public static DBHandler buildDCACompetencyHandler(ProcessorContext context) {
    return new DCACompetencyHandler(context);
  }

  public static DBHandler buildUserTaxonomySubjectHandler(ProcessorContext context) {
    return new UserTaxonomySubjectHandler(context);
  }

  public static DBHandler buildRubricGradingHandler(ProcessorContext context) {
    return new RubricGradingHandler(context);
  }

  public static DBHandler buildScoreUpdateHandler(ProcessorContext context) {
    return new ScoreUpdateHandler(context);
  }

  public static DBHandler buildDCAScoreUpdateHandler(ProcessorContext context) {
    return new DCAScoreUpdateHandler(context);
  }

  public static DBHandler buildStudentSelfReportingHandler(ProcessorContext context) {
    return new StudentSelfReportingHandler(context);
  }

  public static DBHandler buildDCAStudentSelfReportingHandler(ProcessorContext context) {
    return new DCAStudentSelfReportingHandler(context);
  }

  public static DBHandler buildCollectionStartRDAHandler(RDAProcessorContext context) {
    return new CollectionStartEventRDAHandler(context);
  }

  public static DBHandler buildCollectionStopRDAHandler(RDAProcessorContext context) {
    return new CollectionStopEventRDAHandler(context);
  }

  public static DBHandler buildResourceStopRDAHandler(RDAProcessorContext context) {
    return new ResourceStopEventRDAHandler(context);
  }

  public static DBHandler buildCollScoreUpdateRDAHandler(RDAProcessorContext context) {
    return new CollectionScoreUpdateEventRDAHandler(context);
  }

  public static DBHandler buildStudentSelfGradeRDAHandler(RDAProcessorContext context) {
    return new StudentSelfGradeEventRDAHandler(context);
  }

  public static DBHandler buildDCAOfflineStudentReportingHandler(ProcessorContext context) {
    return new DCAOfflineStudentReportingHandler(context);
  }

  public static DBHandler buildOfflineStudentReportingHandler(ProcessorContext context) {
    return new OfflineStudentReportingHandler(context);
  }

  public static DBHandler buildOfflineStudentPerfRDAHandler(RDAProcessorContext context) {
    return new OfflineStudentPerfEventRDAHandler(context);
  }
  
  //TODO: *
  public static DBHandler buildDCARubricGradingHandler(ProcessorContext context) {
    return new DCARubricGradingHandler(context);
  }
  
  public static DBHandler buildDCAPerfUpdateHandler(ProcessorContext context) {
    return new DCAPerfUpdateHandler(context);
  }
  
  public static DBHandler buildOATeacherGradeRDAHandler(RDAProcessorContext context) {
    return new OATeacherGradeEventRDAHandler(context);
  }
}
