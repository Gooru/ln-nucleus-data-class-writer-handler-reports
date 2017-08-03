package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.events.EventParser;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDCACompetencyReport;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DCACompetencyHandler implements DBHandler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DCACompetencyHandler.class);
	  private final ProcessorContext context;
	  private AJEntityDCACompetencyReport dcacompetencyReport;
	  private EventParser event;

	  public DCACompetencyHandler(ProcessorContext context) {
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
	    dcacompetencyReport = new AJEntityDCACompetencyReport();
	    event = context.getEvent();
	    dcacompetencyReport.set(AJEntityDCACompetencyReport.SESSION_ID, event.getSessionId());
	    dcacompetencyReport.set(AJEntityDCACompetencyReport.CLASS_ID, event.getClassGooruId());
	    dcacompetencyReport.set(AJEntityDCACompetencyReport.COURSE_ID, event.getCourseGooruId());
	    dcacompetencyReport.set(AJEntityDCACompetencyReport.UNIT_ID, event.getUnitGooruId());
	    dcacompetencyReport.set(AJEntityDCACompetencyReport.LESSON_ID, event.getLessonGooruId());
	    dcacompetencyReport.set(AJEntityDCACompetencyReport.ACTOR_ID, event.getGooruUUID());
	    dcacompetencyReport.set(AJEntityDCACompetencyReport.TENANT_ID, event.getTenantId());
	    dcacompetencyReport.set(AJEntityDCACompetencyReport.COLLECTION_ID, event.getParentGooruId());
	    dcacompetencyReport.set(AJEntityDCACompetencyReport.RESOURCE_ID, event.getContentGooruId());
	    dcacompetencyReport.set(AJEntityDCACompetencyReport.COLLECTION_TYPE, event.getCollectionType());
	    dcacompetencyReport.set(AJEntityDCACompetencyReport.RESOURCE_TYPE, event.getResourceType());
	    dcacompetencyReport.set(AJEntityDCACompetencyReport.EVENT_TYPE, event.getEventType());
	    dcacompetencyReport.set(AJEntityDCACompetencyReport.CREATED_AT, new Timestamp(event.getStartTime()));
	    dcacompetencyReport.set(AJEntityDCACompetencyReport.UPDATED_AT, new Timestamp(event.getEndTime()));

	    Object dcaReportId = Base.firstCell(AJEntityDailyClassActivity.SELECT_DCA_REPORT_ID, event.getParentGooruId(), event.getSessionId(), event.getContentGooruId(), event.getEventType());
	    if (dcaReportId != null) {
	      int sequenceId = Integer.valueOf(dcaReportId.toString());
	      dcacompetencyReport.set(AJEntityDCACompetencyReport.BASE_REPORT_ID, sequenceId);
	    } else {
	      LOGGER.warn("DCA Report ID can not be null...");
	      return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(), ExecutionStatus.FAILED);
	    }

	    if (dcacompetencyReport.hasErrors()) {
	      LOGGER.error("Errors in creating DCA Competency Report");
	      return new ExecutionResult<>(MessageResponseFactory.createInternalErrorResponse(), ExecutionStatus.FAILED);

	    }

	    List<AJEntityDCACompetencyReport> reports = AJEntityDCACompetencyReport.where(AJEntityDCACompetencyReport.SELECT_ROWS_BY_SESSION_ID_AND_RESOURCE, event.getSessionId(), event.getContentGooruId(), event.getEventType());
	    if (reports.isEmpty()) {
	      List<AJEntityDCACompetencyReport> compentencyReports = new ArrayList<>();
	      if (!event.getTaxonomyIds().isEmpty()) {
	        for (String internalTaxonomyCode : event.getTaxonomyIds().fieldNames()) {
	          AJEntityDCACompetencyReport taxCompetency = new AJEntityDCACompetencyReport();
	          taxCompetency.copyFrom(dcacompetencyReport);
	          String displayCode = event.getTaxonomyIds().getString(internalTaxonomyCode);
	          Map<String, String> taxObject = splitByTaxonomyCode(internalTaxonomyCode);
	          taxCompetency.set(AJEntityDCACompetencyReport.DISPLAY_CODE, displayCode);
	          taxCompetency.set(AJEntityDCACompetencyReport.TAX_SUBJECT_ID, taxObject.get(MessageConstants.SUBJECT));
	          taxCompetency.set(AJEntityDCACompetencyReport.TAX_COURSE_ID, taxObject.get(MessageConstants.COURSE));
	          taxCompetency.set(AJEntityDCACompetencyReport.TAX_DOMAIN_ID, taxObject.get(MessageConstants.DOMAIN));
	          taxCompetency.set(AJEntityDCACompetencyReport.TAX_STANDARD_ID, taxObject.get(MessageConstants.STANDARDS));
	          taxCompetency.set(AJEntityDCACompetencyReport.TAX_MICRO_STANDARD_ID, taxObject.get(MessageConstants.LEARNING_TARGETS));
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

	  private Map<String, String> splitByTaxonomyCode(String taxonomyCode) {
	    int codeLength = taxonomyCode.split(MessageConstants.HYPHEN).length;
	    int subjectIndex = taxonomyCode.indexOf(MessageConstants.HYPHEN);
	    int courseIndex = taxonomyCode.indexOf(MessageConstants.HYPHEN, subjectIndex + 1);
	    int domainIndex = taxonomyCode.indexOf(MessageConstants.HYPHEN, courseIndex + 1);
	    int standardIndex = taxonomyCode.indexOf(MessageConstants.HYPHEN, domainIndex + 1);
	    Map<String, String> taxObject = null;
	    switch (codeLength) {
	    case 1:
	      taxObject = getTaxObject(taxonomyCode.trim(), null, null, null, null);
	      break;
	    case 2:
	      taxObject = getTaxObject(taxonomyCode.substring(0, subjectIndex).trim(), taxonomyCode.trim(), null, null, null);
	      break;
	    case 3:
	      taxObject = getTaxObject(taxonomyCode.substring(0, subjectIndex).trim(), taxonomyCode.substring(0, courseIndex).trim(), taxonomyCode.trim(),
	              null, null);
	      break;
	    case 4:
	      taxObject = getTaxObject(taxonomyCode.substring(0, subjectIndex).trim(), taxonomyCode.substring(0, courseIndex).trim(),
	              taxonomyCode.substring(0, domainIndex).trim(), taxonomyCode.trim(), null);
	      break;
	    case 5:
	      taxObject = getTaxObject(taxonomyCode.substring(0, subjectIndex).trim(), taxonomyCode.substring(0, courseIndex).trim(),
	              taxonomyCode.substring(0, domainIndex).trim(), taxonomyCode.substring(0, standardIndex).trim(), taxonomyCode.trim());
	      break;
	    }
	    LOGGER.debug("taxObject : {} ", taxObject);
	    return taxObject;
	  }

	  private Map<String, String> getTaxObject(String subject, String course, String domain, String standard, String microStandard) {
	    Map<String, String> taxObject = new HashMap<>();
	    taxObject.put(MessageConstants.SUBJECT, subject);
	    taxObject.put(MessageConstants.COURSE, course);
	    taxObject.put(MessageConstants.DOMAIN, domain);
	    taxObject.put(MessageConstants.STANDARDS, standard);
	    taxObject.put(MessageConstants.LEARNING_TARGETS, microStandard);
	    return taxObject;
	  }

}
