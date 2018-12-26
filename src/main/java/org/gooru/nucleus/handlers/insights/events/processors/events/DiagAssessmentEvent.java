package org.gooru.nucleus.handlers.insights.events.processors.events;

import java.io.Serializable;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;


/**
 * @author mukul@gooru
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "assessmentId", "sessionId", "userId", "classId", "score", "questions" })
public class DiagAssessmentEvent implements Serializable {

	@JsonProperty("assessmentId")
	private String assessmentId;
	@JsonProperty("sessionId")
	private String sessionId;
	@JsonProperty("userId")
	private String userId;
	@JsonProperty("classId")
	private String classId;
	@JsonProperty("score")
	private Double score;
	@JsonProperty("questions")
	private List<String> questions = null;
	private final static long serialVersionUID = -8143823714342332871L;

	/**
	 * No args constructor for use in serialization
	 *
	 */
	public DiagAssessmentEvent() {
	}

	/**
	 *
	 * @param classId
	 * @param sessionId
	 * @param userId
	 * @param assessmentId
	 * @param score
	 * @param questions
	 */
	public DiagAssessmentEvent(String assessmentId, String sessionId, String userId, String classId, Double score,
			List<String> questions) {
		super();
		this.assessmentId = assessmentId;
		this.sessionId = sessionId;
		this.userId = userId;
		this.classId = classId;
		this.score = score;
		this.questions = questions;
	}

	@JsonProperty("assessmentId")
	public String getAssessmentId() {
		return assessmentId;
	}

	@JsonProperty("assessmentId")
	public void setAssessmentId(String assessmentId) {
		this.assessmentId = assessmentId;
	}

	public DiagAssessmentEvent withAssessmentId(String assessmentId) {
		this.assessmentId = assessmentId;
		return this;
	}

	@JsonProperty("sessionId")
	public String getSessionId() {
		return sessionId;
	}

	@JsonProperty("sessionId")
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public DiagAssessmentEvent withSessionId(String sessionId) {
		this.sessionId = sessionId;
		return this;
	}

	@JsonProperty("userId")
	public String getUserId() {
		return userId;
	}

	@JsonProperty("userId")
	public void setUserId(String userId) {
		this.userId = userId;
	}

	public DiagAssessmentEvent withUserId(String userId) {
		this.userId = userId;
		return this;
	}

	@JsonProperty("classId")
	public String getClassId() {
		return classId;
	}

	@JsonProperty("classId")
	public void setClassId(String classId) {
		this.classId = classId;
	}

	public DiagAssessmentEvent withClassId(String classId) {
		this.classId = classId;
		return this;
	}

	@JsonProperty("score")
	public Double getScore() {
		return score;
	}

	@JsonProperty("score")
	public void setScore(Double score) {
		this.score = score;
	}

	public DiagAssessmentEvent withScore(Double score) {
		this.score = score;
		return this;
	}

	@JsonProperty("questions")
	public List<String> getQuestions() {
		return questions;
	}

	@JsonProperty("questions")
	public void setQuestions(List<String> questions) {
		this.questions = questions;
	}

	public DiagAssessmentEvent withQuestions(List<String> questions) {
		this.questions = questions;
		return this;
	}
}
