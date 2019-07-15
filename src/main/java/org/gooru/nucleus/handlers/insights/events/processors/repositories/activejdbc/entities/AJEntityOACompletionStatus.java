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
 * @author renuka
 */
@Table("offline_activity_completion_status")
public class AJEntityOACompletionStatus extends Model {

  private static final Logger LOGGER = LoggerFactory.getLogger(AJEntityOACompletionStatus.class);
  
  public static final String ID = "id";
  public static final String OA_ID = "oa_id";
  public static final String OA_DCA_ID = "oa_dca_id";
  public static final String CLASS_ID = "class_id";
  public static final String STUDENT_ID = "student_id";
  public static final String CONTENT_SOURCE = "content_source";
  public static final String COLLECTION_TYPE = "collection_type";
  public static final String MARKED_BY = "marked_by";
  public static final String IS_MARKED_BY_STUDENT = "is_marked_by_student";
  public static final String IS_MARKED_BY_TEACHER = "is_marked_by_teacher";
  public static final String CREATED_AT = "created_at";
  public static final String UPDATED_AT = "updated_at";  
 
  public static final String UPDATE_OA_COMPLETION_STATUS_BY_STUDENT =
      "UPDATE offline_activity_completion_status SET is_marked_by_student = ?, updated_at = ? WHERE id = ? AND content_source = ?";

  public static final String UPDATE_OA_COMPLETION_STATUS_BY_TEACHER =
      "UPDATE offline_activity_completion_status SET is_marked_by_teacher = ?, updated_at = ? WHERE id = ? AND content_source = ?";

  public static final String GET_OA_COMPLETION_STATUS =
      "student_id = ?::uuid AND oa_id = ?::uuid AND oa_dca_id = ? AND class_id = ?::uuid AND content_source = ?";
  
  public static final String GET_OA_MARKED_AS_COMPLETED =
      "student_id = ?::uuid AND oa_id = ?::uuid AND oa_dca_id = ? AND class_id = ?::uuid AND content_source = ? and (is_marked_by_student = true or is_marked_by_teacher = true)";

  private static final Map<String, FieldConverter> converterRegistry;

  static {
    converterRegistry = initializeConverters();
  }

  private static Map<String, FieldConverter> initializeConverters() {
    Map<String, FieldConverter> converterMap = new HashMap<>();
    converterMap.put(STUDENT_ID, (fieldValue -> FieldConverter.convertFieldToUuid((String) fieldValue)));
    converterMap.put(OA_ID, (fieldValue -> FieldConverter.convertFieldToUuid((String) fieldValue)));
    converterMap.put(CLASS_ID, (fieldValue -> FieldConverter.convertFieldToUuid((String) fieldValue)));
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
