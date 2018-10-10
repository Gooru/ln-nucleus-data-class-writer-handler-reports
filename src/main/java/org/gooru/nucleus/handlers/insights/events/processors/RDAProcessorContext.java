package org.gooru.nucleus.handlers.insights.events.processors;

import org.gooru.nucleus.handlers.insights.events.rda.processor.collection.CollectionEventParser;
import org.gooru.nucleus.handlers.insights.events.rda.processor.resource.ResourceEventParser;

import io.vertx.core.json.JsonObject;

/**
 * Created by renuka@gooru
 */
public class RDAProcessorContext {

    private JsonObject request;
    private CollectionEventParser collectionEvent;
    private ResourceEventParser resourceEvent;
    
    public RDAProcessorContext(JsonObject request, CollectionEventParser event) {
        this.request = request != null ? request.copy() : null;
        this.collectionEvent = event;
    }
    
    public RDAProcessorContext(JsonObject request, ResourceEventParser event) {
        this.request = request != null ? request.copy() : null;
        this.resourceEvent = event;
    }

    public JsonObject request() {
        return this.request;
    }
    
    public CollectionEventParser getCollectionEvent (){
    	return this.collectionEvent;
    }
    
    public ResourceEventParser getResourceEvent (){
        return this.resourceEvent;
    }

}
