package org.gooru.nucleus.handlers.insights.events.processors;

import org.gooru.nucleus.handlers.insights.events.processors.events.EventParser;
import io.vertx.core.json.JsonObject;

/**
 * Created by mukul@gooru
 */
public class ProcessorContext {

    private final JsonObject request;
    private final EventParser event;

    public ProcessorContext(JsonObject request, EventParser event) {
        this.request = request != null ? request.copy() : null;  

        this.event = event;
    }
    
    public JsonObject request() {
        return this.request;
    }
    
    public EventParser getEvent (){
    	return this.event;
   }

}
