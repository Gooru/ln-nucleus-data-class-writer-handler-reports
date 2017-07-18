package org.gooru.nucleus.handlers.insights.events.rubrics.processors.repositories.activejdbc.formatter;

import java.util.List;

/**
 * Created by ashish on 20/1/16.
 */
public final class JsonFormatterBuilder {

    public static JsonFormatter buildSimpleJsonFormatter(boolean pretty, List<String> attributes) {

        return new SimpleJsonFormatter(pretty, attributes);
    }
}
