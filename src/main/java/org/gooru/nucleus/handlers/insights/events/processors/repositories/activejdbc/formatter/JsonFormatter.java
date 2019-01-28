package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.formatter;

import org.javalite.activejdbc.LazyList;
import org.javalite.activejdbc.Model;

/**
 * Created by ashish on 20/1/16.
 */
public interface JsonFormatter {

  <T extends Model> String toJson(T model);

  <T extends Model> String toJson(LazyList<T> modelList);
}
