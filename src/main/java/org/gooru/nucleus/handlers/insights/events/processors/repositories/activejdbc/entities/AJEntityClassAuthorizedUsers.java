package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;

@Table("class_authorized_users")
public class AJEntityClassAuthorizedUsers extends Model {

  public static final String CLASS_ID = "class_id";
  public static final String CREATOR_ID = "creator_id";
  public static final String MODIFIED = "modified";

  public static final String SELECT_AUTHORIZED_USER_EXISIST =
      "SELECT * FROM class_authorized_users WHERE class_id = ? AND creator_id = ?";
  public static final String INSERT_AUTHORIZED_USER =
      "INSERT INTO class_authorized_users(class_id,creator_id)VALUES(?,?)";
  public static final String UPDATE_AUTHORIZED_USER =
      "UPDATE class_authorized_users SET creator_id = ? WHERE class_id = ? ";
  public static final String SELECT_CLASS_OWNER = "SELECT * FROM class_authorized_users "
      + "WHERE class_id = ? AND user_id = ?";
}
