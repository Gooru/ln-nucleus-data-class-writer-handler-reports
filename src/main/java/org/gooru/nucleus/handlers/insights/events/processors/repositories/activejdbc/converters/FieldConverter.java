package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.converters;

import java.sql.SQLException;
import org.postgresql.util.PGobject;

/**
 * Created by ashish on 28/1/16.
 */
public interface FieldConverter {

  String UUID_TYPE = "uuid";
  String JSONB_TYPE = "jsonb";
  String TEXT_ARRAY_TYPE = "text[]";
  String TEXT_TYPE = "text";

  static PGobject convertFieldToJson(Object value) {
    PGobject pgObject = new PGobject();
    pgObject.setType(JSONB_TYPE);
    try {
      pgObject.setValue(value == null ? null : String.valueOf(value));
      return pgObject;
    } catch (SQLException e) {
      return null;
    }
  }

  static PGobject convertFieldToUuid(String value) {
    PGobject pgObject = new PGobject();
    pgObject.setType(UUID_TYPE);
    try {
      pgObject.setValue(value);
      return pgObject;
    } catch (SQLException e) {
      return null;
    }
  }

  static PGobject convertFieldToNamedType(Object value, String type) {
    PGobject pgObject = new PGobject();
    pgObject.setType(type);
    try {
      pgObject.setValue(value == null ? null : String.valueOf(value));
      return pgObject;
    } catch (SQLException e) {
      return null;
    }
  }

  static PGobject convertFieldToTextArray(String value) {
    PGobject pgObject = new PGobject();
    pgObject.setType(TEXT_ARRAY_TYPE);
    try {
      pgObject.setValue(value);
      return pgObject;
    } catch (SQLException e) {
      return null;
    }
  }

  static PGobject convertJsonToTextArray(Object value) {
    PGobject pgObject = new PGobject();
    pgObject.setType(TEXT_ARRAY_TYPE);
    try {
      //This is done specifically for gut_codes coming from Core Rubrics since the gut_codes format
      //is updated to Json there.
      String str = value != null ? String.valueOf(value).replace("[", "{").replace("]", "}") : null;
      pgObject.setValue(str == null ? null : str);
      return pgObject;
    } catch (SQLException e) {
      return null;
    }
  }

  static PGobject convertEmptyStringToNull(String s) {
    if (s != null && s.isEmpty()) {
      return null;
    }
    PGobject pgObject = new PGobject();
    pgObject.setType(TEXT_TYPE);
    try {
      pgObject.setValue(s);
      return pgObject;
    } catch (SQLException e) {
      return null;
    }
  }

  PGobject convertField(Object fieldValue);
}
