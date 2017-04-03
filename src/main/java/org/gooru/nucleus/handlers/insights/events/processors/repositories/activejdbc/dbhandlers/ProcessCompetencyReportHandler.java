package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.events.EventParser;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityCompetencyReport;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityReporting;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author daniel
 */
class ProcessCompetencyReportHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessCompetencyReportHandler.class);
  private final ProcessorContext context;
  private AJEntityCompetencyReport competencyReport;
  private EventParser event;

  public ProcessCompetencyReportHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("invalid request received");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid data received to process events"),
              ExecutionStatus.FAILED);
    }

    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {
    competencyReport = new AJEntityCompetencyReport();
    event = context.getEvent();
    competencyReport.set(AJEntityCompetencyReport.SESSION_ID, event.getSessionId());
    competencyReport.set(AJEntityCompetencyReport.CLASS_ID, event.getClassGooruId());
    competencyReport.set(AJEntityCompetencyReport.COURSE_ID, event.getCourseGooruId());
    competencyReport.set(AJEntityCompetencyReport.UNIT_ID, event.getUnitGooruId());
    competencyReport.set(AJEntityCompetencyReport.LESSON_ID, event.getLessonGooruId());
    competencyReport.set(AJEntityCompetencyReport.ACTOR_ID, event.getGooruUUID());
    competencyReport.set(AJEntityCompetencyReport.TENANT_ID, event.getTenantId());
    competencyReport.set(AJEntityCompetencyReport.COLLECTION_ID, event.getParentGooruId());
    competencyReport.set(AJEntityCompetencyReport.RESOURCE_ID, event.getContentGooruId());
    competencyReport.set(AJEntityCompetencyReport.COLLECTION_TYPE, event.getCollectionType());
    competencyReport.set(AJEntityCompetencyReport.CREATED_AT, new Timestamp(event.getStartTime()));
    competencyReport.set(AJEntityCompetencyReport.UPDATED_AT, new Timestamp(event.getEndTime()));

    Object baseReportId = Base.firstCell(AJEntityReporting.SELECT_BASE_REPORT_ID, event.getSessionId(), event.getContentGooruId(), event.getEventType());
    if (baseReportId != null) {
      int sequenceId = Integer.valueOf(baseReportId.toString());
      competencyReport.set(AJEntityCompetencyReport.BASE_REPORT_ID, sequenceId);
    } else {
      LOGGER.warn("Base Report ID can not be null...");
      return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(), ExecutionStatus.FAILED);
    }

    if (competencyReport.hasErrors()) {
      LOGGER.error("Errors in creating Competency Report");
      return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(), ExecutionStatus.FAILED);

    }

    List<AJEntityCompetencyReport> reports = AJEntityCompetencyReport.where(AJEntityCompetencyReport.SELECT_ROWS_BY_SESSION_ID_AND_RESOURCE, event.getSessionId(), event.getContentGooruId());
    if (reports.isEmpty()) {
      List<AJEntityCompetencyReport> compentencyReports = new ArrayList<>();
      if (!event.getTaxonomyIds().isEmpty()) {
        for (String internalTaxonomyCode : event.getTaxonomyIds().fieldNames()) {
          AJEntityCompetencyReport taxCompetency = new AJEntityCompetencyReport();
          taxCompetency.copyFrom(competencyReport);
          String displayCode = event.getTaxonomyIds().getString(internalTaxonomyCode);
          Map<String, String> taxObject = new HashMap<>();
          splitByTaxonomyCode(internalTaxonomyCode, taxObject);
          taxCompetency.set(AJEntityCompetencyReport.DISPLAY_CODE, displayCode);
          taxCompetency.set(AJEntityCompetencyReport.TAX_SUBJECT_ID, taxObject.get(MessageConstants.SUBJECT));
          taxCompetency.set(AJEntityCompetencyReport.TAX_COURSE_ID, taxObject.get(MessageConstants.COURSE));
          taxCompetency.set(AJEntityCompetencyReport.TAX_DOMAIN_ID, taxObject.get(MessageConstants.DOMAIN));
          taxCompetency.set(AJEntityCompetencyReport.TAX_STANDARD_ID, taxObject.get(MessageConstants.STANDARDS));
          taxCompetency.set(AJEntityCompetencyReport.TAX_MICRO_STANDARD_ID, taxObject.get(MessageConstants.MICRO_STANDARDS));
          compentencyReports.add(taxCompetency);
        }
      } else {
        LOGGER.debug("No Taxonomy mapping..");
      }

      compentencyReports.stream().forEach(record -> {
        if (record.isValid()) {
          if (record.insert()) {
            LOGGER.info("Record inserted successfully");
          } else {
            LOGGER.error("Error while inserting competency report: " + context.request().toString());
          }
        }

      });
    } else {
      LOGGER.info("Records already mapped. Don't create duplicates...");
    }
    return new ExecutionResult<>(MessageResponseFactory.createCreatedResponse(), ExecutionStatus.SUCCESSFUL);

  }

  @Override
  public boolean handlerReadOnly() {
    return false;
  }

  private void splitByTaxonomyCode(String taxonomyCode, Map<String, String> taxObject) {
    int codeLength = taxonomyCode.split(MessageConstants.HYPHEN).length;
    int subjectIndex = taxonomyCode.indexOf(MessageConstants.HYPHEN);
    int courseIndex = taxonomyCode.indexOf(MessageConstants.HYPHEN, subjectIndex + 1);
    int domainIndex = taxonomyCode.indexOf(MessageConstants.HYPHEN, courseIndex + 1);
    int standardIndex = taxonomyCode.indexOf(MessageConstants.HYPHEN, domainIndex + 1);
    switch (codeLength) {
    case 1:
      taxObject.put(MessageConstants.SUBJECT, taxonomyCode.trim());
      taxObject.put(MessageConstants.COURSE, null);
      taxObject.put(MessageConstants.DOMAIN, null);
      taxObject.put(MessageConstants.STANDARDS, null);
      taxObject.put(MessageConstants.LEARNING_TARGETS, null);
      break;
    case 2:
      taxObject.put(MessageConstants.SUBJECT, taxonomyCode.substring(0, subjectIndex).trim());
      taxObject.put(MessageConstants.COURSE, taxonomyCode.trim());
      taxObject.put(MessageConstants.DOMAIN, null);
      taxObject.put(MessageConstants.STANDARDS, null);
      taxObject.put(MessageConstants.LEARNING_TARGETS, null);
      break;
    case 3:
      taxObject.put(MessageConstants.SUBJECT, taxonomyCode.substring(0, subjectIndex).trim());
      taxObject.put(MessageConstants.COURSE, taxonomyCode.substring(0, courseIndex).trim());
      taxObject.put(MessageConstants.DOMAIN, taxonomyCode.trim());
      taxObject.put(MessageConstants.STANDARDS, null);
      taxObject.put(MessageConstants.LEARNING_TARGETS, null);
      break;
    case 4:
      taxObject.put(MessageConstants.SUBJECT, taxonomyCode.substring(0, subjectIndex).trim());
      taxObject.put(MessageConstants.COURSE, taxonomyCode.substring(0, courseIndex).trim());
      taxObject.put(MessageConstants.DOMAIN, taxonomyCode.substring(0, domainIndex).trim());
      taxObject.put(MessageConstants.STANDARDS, taxonomyCode.trim());
      taxObject.put(MessageConstants.LEARNING_TARGETS, null);
      break;
    case 5:
      taxObject.put(MessageConstants.SUBJECT, taxonomyCode.substring(0, subjectIndex).trim());
      taxObject.put(MessageConstants.COURSE, taxonomyCode.substring(0, courseIndex).trim());
      taxObject.put(MessageConstants.DOMAIN, taxonomyCode.substring(0, domainIndex).trim());
      taxObject.put(MessageConstants.STANDARDS, taxonomyCode.substring(0, standardIndex).trim());
      taxObject.put(MessageConstants.LEARNING_TARGETS, taxonomyCode.trim());
      break;
    }
    LOGGER.debug("taxObject : {} ", taxObject);
  }

}
