package org.gooru.nucleus.handlers.insights.events.processors.repositories;

import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;

/**
 * @author renuka@gooru
 */
public interface ReportDataAggregateRepo {

  MessageResponse processCollectionStartDataForRDA();

  MessageResponse processCollectionStopDataForRDA();

  MessageResponse processResourceStopDataForRDA();

  MessageResponse processCollScoreUpdateDataForRDA();

  MessageResponse processStudentSelfGradeDataForRDA();

  MessageResponse processOfflineStudentPerfForRDA();

  MessageResponse processOATeacherGradeForRDA();
}
