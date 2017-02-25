package org.gooru.nucleus.handlers.insights.events.processors.repositories;

import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;

/**
 * Created by mukul@gooru
 * Modified by daniel
 */
public interface BaseReportingRepo {
	
	MessageResponse insertBaseReportData();

  MessageResponse reComputeUsageData();

  MessageResponse insertTaxonomyReportData();    
	
  MessageResponse buildClassAuthorizedUser();    
  
  MessageResponse buildCourseCollectionCount();    

}
