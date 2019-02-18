package org.gooru.nucleus.handlers.insights.events.processors.postprocessor.diagnosticassessment;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * @author ashish.
 */

class DiagnosticAssessmentPlayedCommand {

  private final UUID assessmentId;
  private final UUID sessionId;
  private final UUID userId;
  private final UUID classId;
  private final Double score;
  private final List<UUID> questions;
  private List<String> gutCodes;

  DiagnosticAssessmentPlayedCommand(UUID assessmentId, UUID sessionId, UUID userId,
      UUID classId, List<UUID> questions, Double score) {
    this.assessmentId = assessmentId;
    this.sessionId = sessionId;
    this.userId = userId;
    this.classId = classId;
    this.questions = questions;
    this.score = score;
  }

  public static DiagnosticAssessmentPlayedCommand build(JsonObject request) {
    if (request == null || request.isEmpty()) {
      throw new IllegalArgumentException("Invalid/Empty payload for command");
    }
    return buildFromJson(request);
  }

  private static DiagnosticAssessmentPlayedCommand buildFromJson(JsonObject request) {
    UUID assessmentId = fetchFromJsonAsUuid(request, Attributes.ASSESSMENT_ID, false);
    UUID sessionId = fetchFromJsonAsUuid(request, Attributes.SESSION_ID, false);
    UUID userId = fetchFromJsonAsUuid(request, Attributes.USER_ID, false);
    UUID classId = fetchFromJsonAsUuid(request, Attributes.CLASS_ID, true);
    List<UUID> questions = populateQuestions(request.getJsonArray(Attributes.QUESTIONS));
    Double score = request.getDouble(Attributes.SCORE);
    return new DiagnosticAssessmentPlayedCommand(assessmentId, sessionId, userId, classId,
        questions, score);
  }

  private static List<UUID> populateQuestions(JsonArray questionsInPayload) {
    if (questionsInPayload == null || questionsInPayload.isEmpty()) {
      return Collections.emptyList();
    }
    List<UUID> questions = new ArrayList<>(questionsInPayload.size());
    for (Object o : questionsInPayload) {
      questions.add(UUID.fromString(Objects.toString(o)));
    }
    return questions;
  }

  private static UUID fetchFromJsonAsUuid(JsonObject payload, String key, boolean nullable) {
    String value = payload.getString(key);
    if (!nullable && value == null) {
      throw new IllegalArgumentException(
          key + " is non nullable, found null in request: " + Objects.toString(payload));
    } else if (value == null) {
      return null;
    } else {
      return UUID.fromString(value);
    }
  }

  public UUID getAssessmentId() {
    return assessmentId;
  }

  public UUID getSessionId() {
    return sessionId;
  }

  public UUID getUserId() {
    return userId;
  }

  public UUID getClassId() {
    return classId;
  }

  public List<UUID> getQuestions() {
    return questions;
  }

  public List<String> getGutCodes() {
    return gutCodes;
  }

  public void setGutCodes(List<String> gutCodes) {
    this.gutCodes = gutCodes;
  }

  public Double getScore() {
    return score;
  }

  @Override
  public String toString() {
    String gutCodesString = gutCodes != null ? Arrays.toString(gutCodes.toArray()) : "";
    return "DiagnosticAssessmentPlayedCommand{" +
        "assessmentId=" + assessmentId +
        ", sessionId=" + sessionId +
        ", userId=" + userId +
        ", classId=" + classId +
        ", score=" + score +
        ", questions=" + questions +
        ", gutCodes=" + gutCodesString +
        '}';
  }

  private static final class Attributes {

    private static final String ASSESSMENT_ID = "assessmentId";
    private static final String SESSION_ID = "sessionId";
    private static final String USER_ID = "userId";
    private static final String CLASS_ID = "classId";
    private static final String QUESTIONS = "questions";
    private static final String SCORE = "score";

    private Attributes() {
      throw new AssertionError();
    }
  }
}
