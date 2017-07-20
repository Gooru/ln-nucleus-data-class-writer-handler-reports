package org.gooru.nucleus.handlers.insights.events.rubrics.processors.repositories;

import org.gooru.nucleus.handlers.insights.events.rubrics.processors.responses.MessageResponse;

/**
 * Created by mukul@gooru
 */

public interface RubricGradingRepo {
	
	MessageResponse processStudentGrades();

}
