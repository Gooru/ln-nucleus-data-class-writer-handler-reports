package org.gooru.nucleus.handlers.insights.events.processors.grading;

import io.vertx.core.json.JsonObject;


/**
 * @author mukul@gooru
 */
public class GradingContext {

  private JsonObject request;
  public GradingContext(JsonObject request) {
    this.request = request != null ? request.copy() : null;
  }

  public JsonObject request() {
    return this.request;
  }

}
