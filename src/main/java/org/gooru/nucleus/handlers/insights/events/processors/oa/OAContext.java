package org.gooru.nucleus.handlers.insights.events.processors.oa;

import io.vertx.core.json.JsonObject;


/**
 * @author mukul@gooru
 */
public class OAContext {

  private JsonObject request;

  public OAContext(JsonObject request) {
    this.request = request != null ? request.copy() : null;
  }

  public JsonObject request() {
    return this.request;
  }

}
