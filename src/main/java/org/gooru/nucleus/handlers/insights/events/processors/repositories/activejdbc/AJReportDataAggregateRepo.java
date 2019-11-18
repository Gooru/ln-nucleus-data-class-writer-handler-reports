package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc;

import org.gooru.nucleus.handlers.insights.events.processors.RDAProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.ReportDataAggregateRepo;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.DBHandlerBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.transactions.TransactionExecutor;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;

/**
 * Created by renuka
 */
class AJReportDataAggregateRepo implements ReportDataAggregateRepo {

  private final RDAProcessorContext context;

  public AJReportDataAggregateRepo(RDAProcessorContext context) {
    this.context = context;
  }

  @Override
  public MessageResponse processCollectionStartDataForRDA() {
    return TransactionExecutor
        .executeTransaction(DBHandlerBuilder.buildCollectionStartRDAHandler(context));
  }

  @Override
  public MessageResponse processCollectionStopDataForRDA() {
    return TransactionExecutor
        .executeTransaction(DBHandlerBuilder.buildCollectionStopRDAHandler(context));
  }

  @Override
  public MessageResponse processResourceStopDataForRDA() {
    return TransactionExecutor
        .executeTransaction(DBHandlerBuilder.buildResourceStopRDAHandler(context));
  }

  @Override
  public MessageResponse processCollScoreUpdateDataForRDA() {
    return TransactionExecutor
        .executeTransaction(DBHandlerBuilder.buildCollScoreUpdateRDAHandler(context));
  }

  @Override
  public MessageResponse processCollTimespentUpdateDataForRDA() {
    return TransactionExecutor
        .executeTransaction(DBHandlerBuilder.buildCollTimespentUpdateRDAHandler(context));
  }
  
  @Override
  public MessageResponse processStudentSelfGradeDataForRDA() {
    return TransactionExecutor
        .executeTransaction(DBHandlerBuilder.buildStudentSelfGradeRDAHandler(context));
  }

  @Override
  public MessageResponse processOfflineStudentPerfForRDA() {
    return TransactionExecutor
        .executeTransaction(DBHandlerBuilder.buildOfflineStudentPerfRDAHandler(context));
  }
  
  @Override
  public MessageResponse processOATeacherGradeForRDA() {
    return TransactionExecutor
        .executeTransaction(DBHandlerBuilder.buildOATeacherGradeRDAHandler(context));
  }  

}
