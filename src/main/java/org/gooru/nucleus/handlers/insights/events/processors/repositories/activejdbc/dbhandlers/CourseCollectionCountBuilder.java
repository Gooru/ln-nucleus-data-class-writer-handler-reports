package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.dbhandlers;

import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorContext;
import org.gooru.nucleus.handlers.insights.events.processors.events.EventParser;
import org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.entities.AJEntityCourseCollectionCount;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.insights.events.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

/**
 * @author daniel
 */
public class CourseCollectionCountBuilder implements DBHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(CourseCollectionCountBuilder.class);
  private final ProcessorContext context;
  private EventParser event;

  public CourseCollectionCountBuilder(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("invalid request received");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid data received to process events"),
              ExecutionStatus.FAILED);
    }

    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    event = context.getEvent();
    // FIXME : Revisit this logic once we construct the event structure.
    final String eventName = event.getEventName();
    JsonObject source = event.getPayLoadObject().getJsonObject("source");
    JsonObject target = event.getPayLoadObject().getJsonObject("target");
    switch (eventName) {
    case EventConstants.ITEM_DELETE:
      LOGGER.debug("courseId : {}", event.getCourseGooruId());
      LOGGER.debug("unitId : {}", event.getUnitGooruId());
      LOGGER.debug("lessonId : {}", event.getLessonGooruId());
      LOGGER.debug("contentGooruId : {}", event.getContentGooruId());
      LOGGER.debug("contentFormat : {}", event.getContentFormat());
      handleDeleteEvent(event.getCourseGooruId(), event.getUnitGooruId(), event.getLessonGooruId(), event.getContentGooruId(),
              event.getContentFormat());
      LOGGER.debug("CourseCollectionCount update is done..");
      break;
    case EventConstants.ITEM_CREATE:
      // Since we are receiving item.move event while we create
      // collection/assessment. so always process item.move event. Don't process
      // item.create event.
      LOGGER.debug("Nothing to process....");
      break;
    case EventConstants.ITEM_COPY:
      // Current event structure needs to be enhanced.
      LOGGER.debug("target : {}", target);
      if (target != null) {
        handleMoveCopyEvent(target.getString(EventConstants.COURSE_GOORU_OID), target.getString(EventConstants.UNIT_GOORU_OID),
                target.getString(EventConstants.LESSON_GOORU_OID), target.getString(EventConstants.CONTENT_GOORU_OID), event.getContentFormat());
      }
      break;
    case EventConstants.ITEM_MOVE:
      // Current event structure needs to be enhanced.
      LOGGER.debug("source : {}", source);
      LOGGER.debug("target : {}", target);
      if (source != null) {
        handleDeleteEvent(source.getString(EventConstants.COURSE_GOORU_OID), source.getString(EventConstants.UNIT_GOORU_OID),
                source.getString(EventConstants.LESSON_GOORU_OID), source.getString(EventConstants.CONTENT_GOORU_OID), event.getContentFormat());
      }
      if (target != null) {
        handleMoveCopyEvent(target.getString(EventConstants.COURSE_GOORU_OID), target.getString(EventConstants.UNIT_GOORU_OID),
                target.getString(EventConstants.LESSON_GOORU_OID), target.getString(EventConstants.CONTENT_GOORU_OID), event.getContentFormat());
      }
      break;
    default:
      LOGGER.warn("Invalid operation type passed in, not able to handle");
    }
    return new ExecutionResult<>(MessageResponseFactory.createCreatedResponse(), ExecutionStatus.SUCCESSFUL);
  }

  private void handleDeleteEvent(String courseId, String unitId, String lessonId, String leastContentId, String contentFormat) {
    switch (contentFormat) {
    // `-1` indicates decrement 1 from existing value.
    case AJEntityCourseCollectionCount.ATTR_COLLECTION:
      Base.exec(AJEntityCourseCollectionCount.UPDATE_COLLECTION_COUNT, -1, courseId, unitId, lessonId);
      break;
    case AJEntityCourseCollectionCount.ATTR_ASSESSMENT:
      Base.exec(AJEntityCourseCollectionCount.UPDATE_ASSESSMENT_COUNT, -1, courseId, unitId, lessonId);
      break;
    case AJEntityCourseCollectionCount.ATTR_EXTERNAL_ASSESSMENT:
      Base.exec(AJEntityCourseCollectionCount.UPDATE_EXT_ASSESSMENT_COUNT, -1, courseId, unitId, lessonId);
      break;
    case AJEntityCourseCollectionCount.ATTR_COURSE:
      Base.exec(AJEntityCourseCollectionCount.DELETE_COURSE_LEVEL, leastContentId);
      break;
    case AJEntityCourseCollectionCount.ATTR_UNIT:
      Base.exec(AJEntityCourseCollectionCount.DELETE_UNIT_LEVEL, courseId, leastContentId);
      break;
    case AJEntityCourseCollectionCount.ATTR_LESSON:
      Base.exec(AJEntityCourseCollectionCount.DELETE_LESSON_LEVEL, courseId, unitId, leastContentId);
      break;
    default:
      LOGGER.warn("Invalid content format. Please have a look at it.");
    }
  }

  private void handleMoveCopyEvent(String courseId, String unitId, String lessonId, String leastContentId, String contentFormat) {
    boolean rowExist = false;
    Object rowCount = Base.firstCell(AJEntityCourseCollectionCount.SELECT_ROW_COUNT, courseId, unitId, lessonId);
    if (Integer.valueOf(rowCount.toString()) > 0) {
      rowExist = true;
    }
    switch (contentFormat) {
    // `-1` indicates decrement 1 from existing value.
    case AJEntityCourseCollectionCount.ATTR_COLLECTION:
      if (!rowExist) {
        Base.exec(AJEntityCourseCollectionCount.INSERT_COURSE_COLLECTION_COUNT, courseId, unitId, lessonId, 1, 0, 0);
      } else {
        Base.exec(AJEntityCourseCollectionCount.UPDATE_COLLECTION_COUNT, 1, courseId, unitId, lessonId);
      }
      break;
    case AJEntityCourseCollectionCount.ATTR_ASSESSMENT:
      if (!rowExist) {
        Base.exec(AJEntityCourseCollectionCount.INSERT_COURSE_COLLECTION_COUNT,courseId, unitId, lessonId, 0, 1, 0);
      } else {
        Base.exec(AJEntityCourseCollectionCount.UPDATE_ASSESSMENT_COUNT, 1, courseId, unitId, lessonId);
      }
      break;
    case AJEntityCourseCollectionCount.ATTR_EXTERNAL_ASSESSMENT:
      if (!rowExist) {
        Base.exec(AJEntityCourseCollectionCount.INSERT_COURSE_COLLECTION_COUNT, courseId, unitId, lessonId,0, 0, 1);
      } else {
        Base.exec(AJEntityCourseCollectionCount.UPDATE_EXT_ASSESSMENT_COUNT, 1, courseId, unitId, lessonId);
      }
      break;
    case AJEntityCourseCollectionCount.ATTR_COURSE:
      // Do nothing...
      break;
    case AJEntityCourseCollectionCount.ATTR_UNIT:
      // Do nothing...
      break;
    case AJEntityCourseCollectionCount.ATTR_LESSON:
      // Do nothing...
      break;
    default:
      LOGGER.warn("Invalid content format. Please have a look at it.");
    }
  }

  @Override
  public boolean handlerReadOnly() {
    // TODO Auto-generated method stub
    return false;
  }

}
