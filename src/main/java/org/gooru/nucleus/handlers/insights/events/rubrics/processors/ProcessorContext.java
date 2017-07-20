package org.gooru.nucleus.handlers.insights.events.rubrics.processors;

import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonObject;

/**
 * Created by mukul@gooru
 */
public class ProcessorContext {


    private final JsonObject request;
    private final String userId;


    public ProcessorContext(String userId, JsonObject request) {
        this.request = request != null ? request.copy() : null;
        this.userId = userId;        
        
    }
    
    public JsonObject request() {
        return this.request;
    }
    
    public String userId() {
        return this.userId;
    }

}
