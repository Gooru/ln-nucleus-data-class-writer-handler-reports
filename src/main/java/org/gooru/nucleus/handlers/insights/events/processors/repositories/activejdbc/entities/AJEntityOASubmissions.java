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
@Table("offline_activity_submissions")
public class AJEntityOASubmissions extends Model {

  private static final Logger LOGGER = LoggerFactory.getLogger(AJEntityOASubmissions.class);
  
  public static final String ID = "id";  
  public static final String OA_ID = "oa_id";
  public static final String OA_DCA_ID = "oa_dca_id";
  public static final String TASK_ID = "task_id";
  public static final String SUBMISSION_INFO = "submission_info";
  public static final String SUBMISSION_SUBTYPE = "submission_subtype";
  public static final String SUBMISSION_TYPE = "submission_type";
  public static final String SUBMISSION_TEXT = "submission_text";
  public static final String STUDENT_ID = "student_id";
  public static final String CLASS_ID = "class_id";
  public static final String CREATED_AT = "created_at";
  public static final String UPDATED_AT = "updated_at";
  
  public static final String GET_STUDENTS_FOR_OA =
      "SELECT distinct(student_id) from offline_activity_submissions where class_id = ? AND oa_id = ? AND oa_dca_id = ?";
  
  private static final Map<String, FieldConverter> converterRegistry;

  static {
    converterRegistry = initializeConverters();
  }

  private static Map<String, FieldConverter> initializeConverters() {
    Map<String, FieldConverter> converterMap = new HashMap<>();
    converterMap.put(STUDENT_ID, (fieldValue -> FieldConverter.convertFieldToUuid((String) fieldValue)));
    converterMap.put(OA_ID, (fieldValue -> FieldConverter.convertFieldToUuid((String) fieldValue)));
    converterMap.put(OA_DCA_ID, (fieldValue -> FieldConverter.convertFieldToUuid((String) fieldValue)));
    converterMap.put(CLASS_ID, (fieldValue -> FieldConverter.convertFieldToUuid((String) fieldValue)));

    return Collections.unmodifiableMap(converterMap);
  }

  public static ConverterRegistry getConverterRegistry() {
    return new OASubmissionsConverterRegistry();
  }

  private static class OASubmissionsConverterRegistry implements ConverterRegistry {
    @Override
    public FieldConverter lookupConverter(String fieldName) {
      return converterRegistry.get(fieldName);
    }
  }
 
    
}
