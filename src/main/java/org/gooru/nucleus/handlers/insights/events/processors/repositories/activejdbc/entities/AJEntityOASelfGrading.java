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
@Table("offline_activity_self_grades")
public class AJEntityOASelfGrading extends Model {

  private static final Logger LOGGER = LoggerFactory.getLogger(AJEntityOASelfGrading.class);
  
  public static final String ID = "id";
  public static final String OA_ID = "oa_id";
  public static final String OA_DCA_ID = "oa_dca_id";
  public static final String CLASS_ID = "class_id";
  public static final String STUDENT_ID = "student_id";
  public static final String STUDENT_SCORE = "student_score";
  public static final String MAX_SCORE = "max_score";
  public static final String TIMESPENT = "time_spent";  
  public static final String OVERALL_COMMENT = "overall_comment";
  public static final String CATEGORY_SCORE = "category_score";
  public static final String CONTENT_SOURCE = "content_source";
  public static final String CREATED_AT = "created_at";
  public static final String UPDATED_AT = "updated_at";  
 
  public static final String UPDATE_OA_SELF_GRADE_FOR_THIS_STUDENT =
      "UPDATE offline_activity_self_grades SET time_spent = ?, student_score = ?, "
          + "max_score = ?, category_score = ?, overall_comment = ?, content_source = ?, updated_at = ? WHERE id = ?";
  
  public static final String GET_OA_PERFORMANCE =
      "select student_id, student_score, max_score, time_spent "
      + "from offline_activity_self_grades where oa_id = ? AND oa_dca_id = ? AND student_id = ANY(?::uuid[]) AND class_id = ?";

 
  private static final Map<String, FieldConverter> converterRegistry;

  static {
    converterRegistry = initializeConverters();
  }

  private static Map<String, FieldConverter> initializeConverters() {
    Map<String, FieldConverter> converterMap = new HashMap<>();
    converterMap.put(STUDENT_ID, (fieldValue -> FieldConverter.convertFieldToUuid((String) fieldValue)));
    converterMap.put(OA_ID, (fieldValue -> FieldConverter.convertFieldToUuid((String) fieldValue)));
    converterMap.put(CLASS_ID, (fieldValue -> FieldConverter.convertFieldToUuid((String) fieldValue)));
    converterMap.put(CATEGORY_SCORE, (FieldConverter::convertFieldToJson));
    converterMap.put(OVERALL_COMMENT,
        (fieldValue -> FieldConverter.convertEmptyStringToNull((String) fieldValue)));

    return Collections.unmodifiableMap(converterMap);
  }

  public static ConverterRegistry getConverterRegistry() {
    return new OASelfGradingConverterRegistry();
  }

  private static class OASelfGradingConverterRegistry implements ConverterRegistry {
    @Override
    public FieldConverter lookupConverter(String fieldName) {
      return converterRegistry.get(fieldName);
    }
  }
  
}
