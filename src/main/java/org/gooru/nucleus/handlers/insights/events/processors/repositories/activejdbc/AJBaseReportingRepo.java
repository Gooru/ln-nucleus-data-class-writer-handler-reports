  package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc;
  
  import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
  import org.gooru.nucleus.handlers.insights.events.processors.repositories.BaseReportingRepo;
  import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.DBHandlerBuilder;
  import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.transactions.TransactionExecutor;
  import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
  
  /**
   * Created by mukul@gooru
   */
  class AJBaseReportingRepo implements BaseReportingRepo {
    private final ProcessorContext context;
  
    public AJBaseReportingRepo(ProcessorContext context) {
      this.context = context;
    }
  
    @Override
    public MessageResponse insertBaseReportData() {
      return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildProcessEventHandler(context));
    }
  
    @Override
    public MessageResponse insertCompetencyReportsData() {
      return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildCompetencyReportsHandler(context));
    }
  
    @Override
    public MessageResponse insertDCAData() {
      return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildDailyClassActivityEventHandler(context));
    }
  
    @Override
    public MessageResponse insertDCACompetencyData() {
      return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildDCACompetencyHandler(context));
    }
  
    @Override
    public MessageResponse createUserTaxonomySubject() {
      return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildUserTaxonomySubjectHandler(context));
    }
  }
