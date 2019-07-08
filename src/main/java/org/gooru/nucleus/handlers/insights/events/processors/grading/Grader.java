package org.gooru.nucleus.handlers.insights.events.processors.grading;

import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;

/**
 * @author mukul@gooru
 */
public abstract class Grader {
  protected GradingContext context;
  
   
  public GradingContext getContext() {
    return context;
  }

  public void setContext(GradingContext context) {
    this.context = context;
  }
  
  public String getType() {
    return context.request().getString(MessageConstants.COLLECTION_TYPE);
  }
  
  public String getContentSource() {
    return context.request().getString(MessageConstants.CONTENT_SOURCE);
  }



  public  MessageResponse process() {
    MessageResponse response = null;
    
    String type = getType();
    String contentSource = getContentSource();
    
    if (type.equals(EventConstants.OFFLINE_ACTIVITY) && 
        contentSource.equals(EventConstants.DCA)) {
      response = processDCAOfflineActivity();     
    } else {
      response =  MessageResponseFactory.createInvalidRequestResponse();
    }
    return response;
  }
  
  protected abstract MessageResponse processDCAOfflineActivity();
}
