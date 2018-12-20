package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.formatter;

import java.util.List;

/**
 * Created by ashish on 20/1/16.
 */
public final class JsonFormatterBuilder {

  private JsonFormatterBuilder() {
  }

  public static JsonFormatter buildSimpleJsonFormatter(boolean pretty, List<String> attributes) {

    return new SimpleJsonFormatter(pretty, attributes);
  }
}
