package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers.eventdispatcher;

import java.util.List;

import org.gooru.nucleus.handlers.insights.events.bootstrap.EBSendVerticle;
import org.gooru.nucleus.handlers.insights.events.processors.events.DiagAssessmentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.vertx.core.json.JsonObject;

/**
 * @author mukul@gooru
 */
public class DiagnosticEventDispatcher {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DiagnosticEventDispatcher.class);
	
	private DiagAssessmentEvent diagAssessmentEventObject = new DiagAssessmentEvent();
	private JsonObject diagAssessmentEventJson;
	private String assessmentId;
	private String sessionId;
	private String userId;
	private String classId;
	private Double score;
	private List<String> questions;
	
	public DiagnosticEventDispatcher (String assessmentId, String sessionId, String userId, String classId, Double score,
			List<String> questions) {
		this.assessmentId = assessmentId;
		this.sessionId = sessionId;
		this.userId = userId;
		this.classId = classId;
		this.score = score;
		this.questions = questions;
	}
	
	public void dispatchDiagnosticEvent() {
		LOGGER.debug("Build Diagnostic Assessment Event");
		diagAssessmentEventJson = build();
		LOGGER.debug("Send Event for Post Processing");
		send();		
	}

	private JsonObject build() {		
		diagAssessmentEventObject.setAssessmentId(assessmentId);
		diagAssessmentEventObject.setSessionId(sessionId);
		diagAssessmentEventObject.setUserId(userId);
		diagAssessmentEventObject.setClassId(classId);
		diagAssessmentEventObject.setScore(score);
		diagAssessmentEventObject.setQuestions(questions);		
		try {
			String diagEvent = new ObjectMapper().writeValueAsString(diagAssessmentEventObject);
			return (new JsonObject(diagEvent));
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			LOGGER.error("Error creating Diagnostic Event for User: '{}', Diagnostic Assessment: '{}' and Session: '{}' at Class: '{}'", 
					userId, assessmentId, sessionId, classId);
			return (new JsonObject());
		}
	}

	private void send() {		
		if (!diagAssessmentEventJson.isEmpty() && diagAssessmentEventJson != null) {
		    EBSendVerticle EBSender = new EBSendVerticle();
		    EBSender.sendMessage(diagAssessmentEventJson);
			LOGGER.info("Diagnostic Event Dispatched for User: '{}', Diagnostic Assessment: '{}' and Session: '{}' at Class: '{}'", 
					userId, assessmentId, sessionId, classId);
		}		
	}
}
