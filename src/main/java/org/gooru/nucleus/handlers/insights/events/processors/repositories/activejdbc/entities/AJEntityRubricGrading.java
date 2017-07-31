package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.converters.ConverterRegistry;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.converters.FieldConverter;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.validators.FieldValidator;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.validators.ValidatorRegistry;
import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mukul@gooru
 *
 */

@Table("student_rubric_grading")
public class AJEntityRubricGrading extends Model {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AJEntityRubricGrading.class);

    public static final String ID = "id";
    public static final String EVENT_NAME = "event_name";
    public static final String RUBRIC_ID = "rubric_id";
    public static final String TITLE = "title";
    public static final String URL = "url";    
    public static final String DESCRIPTION = "description";
    public static final String METADATA = "metadata";
    public static final String TAXONOMY = "taxonomy";
    public static final String GUT_CODES = "gut_codes";
    
    public static final String CREATOR_ID = "creator_id";
    public static final String MODIFIER_ID = "modifier_id";
    public static final String ORIGINAL_CREATOR_ID = "original_creator_id";
    public static final String ORIGINAL_RUBRIC_ID = "original_rubric_id";
    public static final String PARENT_RUBRIC_ID = "parent_rubric_id";
    public static final String PUBLISH_DATE = "publish_date";
    public static final String RUBRIC_CREATED_AT = "rubric_created_at";
    public static final String RUBRIC_UPDATED_AT = "rubric_updated_at";

    public static final String TENANT = "tenant";
    public static final String TENANT_ROOT = "tenant_root";
    
    
    public static final String STUDENT_ID = "student_id";
    public static final String CLASS_ID = "class_id";
    public static final String COURSE_ID = "course_id";
    public static final String UNIT_ID = "unit_id";
    public static final String LESSON_ID = "lesson_id";
    public static final String COLLECTION_ID = "collection_id";
    public static final String SESSION_ID = "session_id";
    public static final String RESOURCE_ID = "content_id";
    
    public static final String MAX_SCORE = "max_score";
    public static final String STUDENT_SCORE = "student_score";
    public static final String CATEGORY_SCORE = "category_score";
    
    public static final String OVERALL_COMMENT = "overall_comment";    
    public static final String GRADER = "grader";
    public static final String GRADER_ID = "grader_id";
    
    public static final String CREATE_TIMESTAMP = "created_at";
    public static final String UPDATE_TIMESTAMP = "updated_at";   

    
    public static final List<String> VALID_GRADER = Arrays.asList("Self", "Teacher");  
    
    private static final Map<String, FieldValidator> validatorRegistry;
    private static final Map<String, FieldConverter> converterRegistry;

    static {
        validatorRegistry = initializeValidators();
        converterRegistry = initializeConverters();
    }

    private static Map<String, FieldConverter> initializeConverters() {
        Map<String, FieldConverter> converterMap = new HashMap<>();

        converterMap.put(METADATA, (FieldConverter::convertFieldToJson));
        converterMap.put(TAXONOMY, (FieldConverter::convertFieldToJson));
        converterMap.put(DESCRIPTION, (fieldValue -> FieldConverter.convertEmptyStringToNull((String) fieldValue)));        
        converterMap.put(GUT_CODES, (fieldValue -> FieldConverter.convertFieldToTextArray((String) fieldValue)));
        
        converterMap.put(CATEGORY_SCORE, (FieldConverter::convertFieldToJson));
        converterMap.put(OVERALL_COMMENT,
                (fieldValue -> FieldConverter.convertEmptyStringToNull((String) fieldValue)));
        
        return Collections.unmodifiableMap(converterMap);
    }

    private static Map<String, FieldValidator> initializeValidators() {
        Map<String, FieldValidator> validatorMap = new HashMap<>();

        validatorMap.put(TITLE, (value) -> FieldValidator.validateString(value, 1000));
        validatorMap.put(DESCRIPTION, (value) -> FieldValidator.validateStringAllowNullOrEmpty(value, 20000));
        validatorMap.put(METADATA, FieldValidator::validateJsonIfPresent);
        validatorMap.put(TAXONOMY, FieldValidator::validateJsonIfPresent);
        
        validatorMap.put(URL, (value) -> FieldValidator.validateStringIfPresent(value, 2000));
        return Collections.unmodifiableMap(validatorMap);
    }

    public static ValidatorRegistry getValidatorRegistry() {
        return new RubricValidationRegistry();
    }

    public static ConverterRegistry getConverterRegistry() {
        return new RubricConverterRegistry();
    }

    private static class RubricValidationRegistry implements ValidatorRegistry {
        @Override
        public FieldValidator lookupValidator(String fieldName) {
            return validatorRegistry.get(fieldName);
        }
    }

    private static class RubricConverterRegistry implements ConverterRegistry {
        @Override
        public FieldConverter lookupConverter(String fieldName) {
            return converterRegistry.get(fieldName);
        }
    }
    
    private void setPGObject(String field, String type, String value) {
        PGobject pgObject = new PGobject();
        pgObject.setType(type);
        try {    
            pgObject.setValue(value);            
            this.set(field, pgObject);
        } catch (SQLException e) {            
            this.errors().put(field, value);
        }
    }
 

}
