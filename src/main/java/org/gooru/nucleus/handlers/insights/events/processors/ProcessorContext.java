package org.gooru.nucleus.handlers.insights.events.processors;

import org.gooru.nucleus.handlers.insights.events.processors.events.EventParser;
import io.vertx.core.json.JsonObject;

/**
 * Created by mukul@gooru
 */
public class ProcessorContext {

    private JsonObject request;
    private EventParser event;
    private String userId;
    
    public ProcessorContext(JsonObject request, EventParser event) {
        this.request = request != null ? request.copy() : null;  

        this.event = event;
    }
    
    public ProcessorContext(String userId, JsonObject request) {
      this.request = request != null ? request.copy() : null;
      this.userId = userId;        
      
  }

    public JsonObject request() {
        return this.request;
    }
    
    public EventParser getEvent (){
    	return this.event;
   }
   
    public String userId() {
      return this.userId;
    }

}
