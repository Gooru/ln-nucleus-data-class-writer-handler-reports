package org.gooru.nucleus.handlers.insights.events.processors.postprocessor.diagnosticassessment;

import org.gooru.nucleus.handlers.insights.events.app.components.DataSourceRegistry;
import org.javalite.activejdbc.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ashish.
 */

public class LearnerProfileDao {

  private static final int STATUS_COMPLETED = 4;
  private static final Logger LOGGER = LoggerFactory.getLogger(LearnerProfileDao.class);
  private DiagnosticAssessmentPlayedCommand command;
  private DB dsDb;


  public void updateLPCS(DiagnosticAssessmentPlayedCommand command) {
    this.command = command;
    dsDb = new DB("dsDb");
    try {
      LOGGER.debug("LPCS Update will be done");
      dsDb.open(DataSourceRegistry.getInstance().getDsDataSource());
      dsDb.openTransaction();
      LOGGER.debug("Will try to update LPCS");
      updateLPCSBase();
      LOGGER.debug("Will try to update LPCS TS");
      updateLPCSTs();
      LOGGER.debug("Will try to update LPCE");
      updateLPCE();
      LOGGER.debug("Will try to update LPCE TS");
      updateLPCETs();
      dsDb.commitTransaction();
    } catch (Throwable throwable) {
      LOGGER.warn("Exception updating LP. Aborting", throwable);
      dsDb.rollbackTransaction();
    } finally {
      dsDb.close();
    }
  }

  private static final String UPDATE_LPCE_TS =
      "INSERT INTO learner_profile_competency_evidence_ts(user_id, gut_code, class_id, latest_session_id, "
          + " collection_id, collection_score, collection_type, status, created_at, updated_at) "
          + " VALUES (?, ?, ?, ?, ?, ?,  'assessment', ?, now(), now()) ON CONFLICT (user_id, gut_code, collection_id, status) "
          + " DO UPDATE SET latest_session_id = ?, collection_score = ?, updated_at = now()";


  private void updateLPCETs() {
    if (command.getGutCodes() != null && !command.getGutCodes().isEmpty()) {
      LOGGER.debug("Updating LPCE Ts for specified gut codes");
      for (String gutCode : command.getGutCodes()) {
        LOGGER.debug("Update for gut code: '{}'", gutCode);
        dsDb.exec(UPDATE_LPCE_TS, command.getUserId().toString(), gutCode,
            command.getClassId().toString(), command.getSessionId().toString(),
            command.getAssessmentId().toString(), command.getScore(), STATUS_COMPLETED,
            command.getSessionId(),
            command.getScore());
      }
    } else {
      LOGGER.debug("No gut codes to update");
    }
  }

  private static final String UPDATE_LPCE =
      "INSERT INTO learner_profile_competency_evidence(user_id, gut_code, class_id, "
          + " latest_session_id, collection_id, collection_score, collection_type, created_at, updated_at) "
          + " VALUES (?, ?, ?, ?, ?, ?, 'assessment', now(), now()) ON CONFLICT (user_id, gut_code, collection_id) "
          + " DO UPDATE SET latest_session_id = ?, class_id = ?, collection_score = ?, updated_at = now()";

  private void updateLPCE() {
    if (command.getGutCodes() != null && !command.getGutCodes().isEmpty()) {
      LOGGER.debug("Updating LPCE for specified gut codes");
      for (String gutCode : command.getGutCodes()) {
        LOGGER.debug("Update for gut code: '{}'", gutCode);
        dsDb.exec(UPDATE_LPCE, command.getUserId().toString(), gutCode,
            command.getClassId().toString(), command.getSessionId().toString(),
            command.getAssessmentId().toString(), command.getScore(), command.getSessionId(),
            command.getClassId(), command.getScore());
      }
    } else {
      LOGGER.debug("No gut codes to update");
    }

  }

  private static final String UPDATE_LPCS_TS =
      "INSERT INTO learner_profile_competency_status_ts(tx_subject_code, user_id, gut_code, status, created_at, updated_at) "
          + " VALUES(?, ?, ?, ?, now(), now()) ON CONFLICT (user_id, gut_code, status) DO NOTHING";

  private void updateLPCSTs() {
    if (command.getGutCodes() != null && !command.getGutCodes().isEmpty()) {
      LOGGER.debug("Updating LPCS TS for specified gut codes");
      for (String gutCode : command.getGutCodes()) {
        LOGGER.debug("Update for gut code: '{}'", gutCode);
        dsDb.exec(UPDATE_LPCS_TS, command.getSubjectCode(), command.getUserId().toString(), gutCode,
            STATUS_COMPLETED);
      }
    } else {
      LOGGER.debug("No gut codes to update");
    }

  }


  private static final String UPDATE_LPCS =
      "INSERT INTO learner_profile_competency_status(tx_subject_code, user_id, gut_code, status, profile_source, created_at, updated_at) "
          + " VALUES(?, ?, ?, ?, ?, now(), now()) ON CONFLICT (user_id, gut_code) "
          + " DO UPDATE SET status = ?, updated_at = now() WHERE learner_profile_competency_status.status <= EXCLUDED.status";

  private void updateLPCSBase() {
    if (command.getGutCodes() != null && !command.getGutCodes().isEmpty()) {
      LOGGER.debug("Updating LPCS for specified gut codes");
      for (String gutCode : command.getGutCodes()) {
        LOGGER.debug("Update for gut code: '{}'", gutCode);
        dsDb.exec(UPDATE_LPCS, command.getSubjectCode(), command.getUserId().toString(), gutCode,
            STATUS_COMPLETED, command.getProfileSource(), STATUS_COMPLETED);
      }
    } else {
      LOGGER.debug("No gut codes to update");
    }

  }

}
