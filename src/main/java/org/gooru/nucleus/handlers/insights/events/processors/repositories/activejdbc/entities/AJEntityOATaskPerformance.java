package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.converters.ConverterRegistry;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.converters.FieldConverter;
import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** 
 * @author mukul@gooru
 */
@Table("offline_activity_task_self_grades")
public class AJEntityOATaskPerformance extends Model {

  private static final Logger LOGGER = LoggerFactory.getLogger(AJEntityOASubmissions.class);
  
  public static final String ID = "id";
  public static final String OA_ID = "oa_id";
  public static final String TASK_ID = "task_id";
  public static final String CLASS_ID = "class_id";
  public static final String STUDENT_ID = "student_id";
  public static final String STUDENT_SCORE = "student_score";
  public static final String MAX_SCORE = "max_score";
  public static final String TIMESPENT = "time_spent";  
  public static final String OVERALL_COMMENT = "overall_comment";
  public static final String CATEGORY_GRADE = "category_grade";
  public static final String CONTENT_SOURCE = "content_source";
  public static final String CREATED_AT = "created_at";
  public static final String UPDATED_AT = "updated_at";  
  
  public static final String GET_OA_PERFORMANCE =
      "select student_id, SUM(student_score) AS student_score, SUM(max_score) AS max_score, SUM(time_spent) AS time_spent "
      + "from offline_activity_task_perf where oa_id = ? and student_id = ANY(?::uuid[]) GROUP BY student_id";

  private static final Map<String, FieldConverter> converterRegistry;

  static {
    converterRegistry = initializeConverters();
  }

  private static Map<String, FieldConverter> initializeConverters() {
    Map<String, FieldConverter> converterMap = new HashMap<>();
    converterMap.put(CATEGORY_GRADE, (FieldConverter::convertFieldToJson));
    converterMap.put(OVERALL_COMMENT,
        (fieldValue -> FieldConverter.convertEmptyStringToNull((String) fieldValue)));

    return Collections.unmodifiableMap(converterMap);
  }

  public static ConverterRegistry getConverterRegistry() {
    return new OASelfGradeConverterRegistry();
  }

  private static class OASelfGradeConverterRegistry implements ConverterRegistry {
    @Override
    public FieldConverter lookupConverter(String fieldName) {
      return converterRegistry.get(fieldName);
    }
  }

}

