package org.gooru.nucleus.handlers.insights.events.processors.postprocessor.diagnosticassessment;

import io.vertx.core.json.JsonObject;
import org.gooru.nucleus.handlers.insights.events.app.components.DataSourceRegistry;
import org.javalite.activejdbc.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ashish.
 */

public class DiagnosticQueueAndStatusUpdaterDao {

  private static final int STATUS_COMPLETED = 4;
  private static final Logger LOGGER = LoggerFactory
      .getLogger(DiagnosticQueueAndStatusUpdaterDao.class);
  private DiagnosticAssessmentPlayedCommand command;
  private JsonObject payload;
  private DB coreDb;


  public void queueRecordAndUpdateStatus(JsonObject request,
      DiagnosticAssessmentPlayedCommand command) {

    this.command = command;
    this.payload = request;

    coreDb = new DB("coreDb");
    try {
      LOGGER.debug("ILP payload will be queued");
      coreDb.open(DataSourceRegistry.getInstance().getCoreDataSource());
      coreDb.openTransaction();

      queueRecord();

      updateStatus();

      coreDb.commitTransaction();
    } catch (Throwable throwable) {
      LOGGER.warn("Exception queueing payload in core for ILP. Aborting", throwable);
      coreDb.rollbackTransaction();
    } finally {
      coreDb.close();
    }
  }

  private void updateStatus() {
    LOGGER.debug("Will try to update status");
    int count = coreDb.exec(UPDATE_DIAGNOSTIC_STATUS_DONE, command.getClassId().toString(),
        command.getUserId().toString());
    if (count < 1) {
      LOGGER.warn("Not able to queue record for ILP for payload: ");
      LOGGER.warn(payload.toString());
    } else {
      LOGGER.debug("Queued record for processing: {}", payload.toString());
    }
  }

  private void queueRecord() {
    LOGGER.debug("Will try to queue record");
    int count = coreDb.exec(QUEUE_ILP_RECORD, command.getUserId().toString(), payload.toString(),
        command.getClassId().toString());
    if (count < 1) {
      LOGGER.warn("Not able to update status for ILP for payload: ");
      LOGGER.warn(payload.toString());
    } else {
      LOGGER.debug("Updated status for ILP request: {}", payload.toString());
    }
  }

  private static final String QUEUE_ILP_RECORD =
      "insert into skyline_initial_queue (user_id, class_id, course_id, status, category, payload, created_at, updated_at) "
          + "  select ?::uuid, id, course_id, 0, 0, ?, now(), now() from class where id = ?::uuid";
  private static final String UPDATE_DIAGNOSTIC_STATUS_DONE =
      "update class_member set diag_asmt_state = 3 where class_id = ?::uuid and user_id = ?::uuid";

}
