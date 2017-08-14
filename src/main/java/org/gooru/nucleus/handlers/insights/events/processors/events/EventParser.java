package org.gooru.nucleus.handlers.insights.events.processors.events;

import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public final class EventParser {
	
		private JsonObject event;
		
		private static final Logger LOGGER = LoggerFactory.getLogger(EventParser.class);
		
    public EventParser(String json) throws Exception {
			this.event = new JsonObject(json);
			this.parse();
		}

		private String eventId;

		private String eventName;

		private JsonObject context;

		private JsonObject user;

		private JsonObject payLoadObject;

		private JsonObject metrics;

		private JsonObject session;

		private JsonObject version;

		private Long startTime;

		private Long endTime;

		private String apiKey;

		private String contentGooruId;

		private String fields;

		private String parentEventId;

		private String gooruUUID;

		private String lessonGooruId;

		private String unitGooruId;

		private String courseGooruId;

		private String classGooruId;

		private String parentGooruId;

		private String collectionItemId;

		private String sessionId;

		private String eventType;

		private String collectionType;

		private String questionType;

		private String resourceType;

		private JsonArray answerObject;

		private String answerStatus;

		private String gradeType;

		private String gradeStatus;

		private String teacherId;

		private boolean isStudent;

		private JsonObject taxonomyIds;

		private String contentFormat;

		private long eventTime;

		private double score;

		private long timespent;
		
		private long views;
		
		private int reaction;

		private String reportsContext;
		
		private int questionCount;

		private String tenantId;
		
		private String contentSource;
		
		private String appId;
		
		private String partnerId;
		
		private String collectionSubType;

    private double maxScore;		

    private long pathId;
                
    private String timezone;      
    
    private String sourceId;
    
    private String accessToken;
    
		public String getSourceId() {
      return sourceId;
    }

    public void setSourceId(String sourceId) {
      this.sourceId = sourceId;
    }

    public String getAccessToken() {
      return accessToken;
    }

    public void setAccessToken(String accessToken) {
      this.accessToken = accessToken;
    }

    public String getEventId() {
			return eventId;
		}

		public void setEventId(String eventId) {
			this.eventId = eventId;
		}

		public String getEventName() {
			return eventName;
		}

		public void setEventName(String eventName) {
			this.eventName = eventName;
		}

		public JsonObject getContext() {
			return context;
		}

		public void setContext(JsonObject context) {
			this.context = context;
		}

		public JsonObject getUser() {
			return user;
		}

		public void setUser(JsonObject user) {
			this.user = user;
		}

		public JsonObject getPayLoadObject() {
			return payLoadObject;
		}

		public void setPayLoadObject(JsonObject payLoadObject) {
			this.payLoadObject = payLoadObject;
		}

		public JsonObject getMetrics() {
			return metrics;
		}

		public void setMetrics(JsonObject metrics) {
			this.metrics = metrics;
		}

		public JsonObject getSession() {
			return session;
		}

		public void setSession(JsonObject session) {
			this.session = session;
		}

		public Long getStartTime() {
			return startTime;
		}

		public void setStartTime(Long startTime) {
			this.startTime = startTime;
		}

		public Long getEndTime() {
			return endTime;
		}

		public void setEndTime(Long endTime) {
			this.endTime = endTime;
		}

		public String getApiKey() {
			return apiKey;
		}

		public void setApiKey(String apiKey) {
			this.apiKey = apiKey;
		}

		public String getContentGooruId() {
			return contentGooruId;
		}

		public void setContentGooruId(String contentGooruId) {
			this.contentGooruId = contentGooruId;
		}

		public String getFields() {
			return fields;
		}

		public void setFields(String fields) {
			this.fields = fields;
		}

		public JsonObject getVersion() {
			return version;
		}

		public void setVersion(JsonObject version) {
			this.version = version;
		}

		public String getGooruUUID() {
			return gooruUUID;
		}

		public void setGooruUUID(String gooruUUID) {
			this.gooruUUID = gooruUUID;
		}

		public JsonObject getEvent() {
			return event;
		}

		public void setEvent(JsonObject event) {
			this.event = event;
		}

		public String getLessonGooruId() {
			return lessonGooruId;
		}

		public void setLessonGooruId(String lessonGooruId) {
			this.lessonGooruId = lessonGooruId;
		}

		public String getUnitGooruId() {
			return unitGooruId;
		}

		public void setUnitGooruId(String unitGooruId) {
			this.unitGooruId = unitGooruId;
		}

		public String getCourseGooruId() {
			return courseGooruId;
		}

		public void setCourseGooruId(String courseGooruId) {
			this.courseGooruId = courseGooruId;
		}

		public String getClassGooruId() {
			return classGooruId;
		}

		public void setClassGooruId(String classGooruId) {
			this.classGooruId = classGooruId;
		}

		public String getParentGooruId() {
			return parentGooruId;
		}

		public void setParentGooruId(String parentGooruId) {
			this.parentGooruId = parentGooruId;
		}

		public String getCollectionItemId() {
			return collectionItemId;
		}

		public void setCollectionItemId(String collectionItemId) {
			this.collectionItemId = collectionItemId;
		}

		public String getSessionId() {
			return sessionId;
		}

		public void setSessionId(String sessionId) {
			this.sessionId = sessionId;
		}

		public String getEventType() {
			return eventType;
		}

		public void setEventType(String eventType) {
			this.eventType = eventType;
		}

		public String getCollectionType() {
			return collectionType;
		}

		public void setCollectionType(String collectionType) {
			this.collectionType = collectionType;
		}

		public String getQuestionType() {
			return questionType;
		}

		public void setQuestionType(String questionType) {
			this.questionType = questionType;
		}

		public String getResourceType() {
			return resourceType;
		}

		public void setResourceType(String resourceType) {
			this.resourceType = resourceType;
		}

		public JsonArray getAnswerObject() {
			return answerObject;
		}

		public void setAnswerObject(JsonArray answerObject) {
			this.answerObject = answerObject;
		}

		public String getAnswerStatus() {
			return answerStatus;
		}

		public void setAnswerStatus(String answerStatus) {
			this.answerStatus = answerStatus;
		}

		public String getGradeType() {
			return gradeType;
		}

		public void setGradeType(String gradeType) {
			this.gradeType = gradeType;
		}

		public JsonObject getTaxonomyIds() {
			return taxonomyIds;
		}

		public void setTaxonomyIds(JsonObject taxonomyIds) {
			this.taxonomyIds = taxonomyIds;
		}

		public long getEventTime() {
			return eventTime;
		}

		public void setEventTime(long eventTime) {
			this.eventTime = eventTime;
		}

		public double getScore() {
			return score;
		}

		public void setScore(double score) {
			this.score = score;
		}

		public long getTimespent() {
			return timespent;
		}
		
		public void setTimespent(long timespent) {
			this.timespent = timespent;
		}
		

		public long getViews() {
			return views;
		}
		

		public void setViews(long views) {
			this.views = views;
		}
		
		public long getReaction() {
			return reaction;
		}

		public void setReaction(int reaction) {
			this.reaction = reaction;
		}
		public String getGradeStatus() {
			return gradeStatus;
		}

		public void setGradeStatus(String gradeStatus) {
			this.gradeStatus = gradeStatus;
		}

		public String getTeacherId() {
			return teacherId;
		}
		
		public void setTeacherId(String teacherId) {
			this.teacherId = teacherId;
		}

		public void setQuestionCount(int qc) {
			this.questionCount = qc;
		}

		public int getQuestionCount() {
			return questionCount;
		}
		
		public void setContentSource(String contentSource){
			this.contentSource = contentSource;
		}
		
		public String getContentSource(){
			return contentSource;
		}
		
		
		public String getParentEventId() {
			return parentEventId;
		}

		public void setParentEventId(String parentEventId) {
			this.parentEventId = parentEventId;
		}

		public String getContentFormat() {
			return contentFormat;
		}

		public void setContentFormat(String contentFormat) {
			this.contentFormat = contentFormat;
		}

		public String getReportsContext() {
			return reportsContext;
		}

		public void setReportsContext(String reportsContext) {
			this.reportsContext = reportsContext;
		}

		public boolean isStudent() {
			return isStudent;
		}

		private void setStudent(boolean isStudent) {
			this.isStudent = isStudent;
		}
		
		public String getTenantId() {
		      return tenantId;
		}
	
        public void setTenantId(String tenantId) {
        	this.tenantId = tenantId;
        }
        
        public String getCollectionSubType() {
            return collectionSubType;
        }
               
        public void setCollectionSubType(String collectionSubType) {
            this.collectionSubType = collectionSubType;
        }
        
        public String getPartnerId() {
            return partnerId;
        }
        
        public void setPartnerId(String partnerId) {
            this.partnerId = partnerId;
        }
        
        public String getAppId() {
            return appId;
        }
        
        public void setAppId(String appId) {
            this.appId = appId;
        }
        
        public double getMaxScore() {
            return maxScore;
        }
        
        public void setMaxScore(double maxScore) {
            this.maxScore = maxScore;
        }
          
        public long getPathId() {
            return pathId;
        }

        public void setPathId(long pathId) {
            this.pathId = pathId;
        }

        public String getTimeZone() {
            return timezone;
        }

        public void setTimeZone(String timezone) {
            this.timezone = timezone;
        }

        
		private EventParser parse() {
			try {			
							
				this.context = this.event.getJsonObject(EventConstants.CONTEXT);
				if(this.context == null){
				  this.context = new JsonObject();
				}
				this.user = this.event.getJsonObject(EventConstants.USER);
				this.payLoadObject = this.event.getJsonObject(EventConstants.PAY_LOAD);
				if(this.payLoadObject == null){
          this.payLoadObject = new JsonObject();
        }
				this.metrics = this.event.getJsonObject(EventConstants.METRICS);
				if(this.metrics == null){
          this.metrics = new JsonObject();
        }
				this.session = this.event.getJsonObject(EventConstants.SESSION);
				if(this.session == null){
          this.session = new JsonObject();
        }
				this.version = this.event.getJsonObject(EventConstants.VERSION);
				
				this.startTime = this.event.getLong(EventConstants.START_TIME);
				this.endTime = this.event.getLong(EventConstants.END_TIME);
				this.eventId = this.event.getString(EventConstants.EVENT_ID);
				this.eventName = this.event.getString(EventConstants.EVENT_NAME);
				
				this.views = 0;
				this.timespent = 0;
				this.apiKey = session.containsKey(EventConstants.API_KEY) ? session.getString(EventConstants.API_KEY) : EventConstants.NA;
				this.contentGooruId = context.containsKey(EventConstants.CONTENT_GOORU_OID) ? context.getString(EventConstants.CONTENT_GOORU_OID) : EventConstants.NA;

				this.gooruUUID = user.getString(EventConstants.GOORUID);
                this.tenantId = context.containsKey(EventConstants.TENANT_ID) ? context.getString(EventConstants.TENANT_ID) : null;
				this.lessonGooruId = context.containsKey(EventConstants.LESSON_GOORU_OID) ? context.getString(EventConstants.LESSON_GOORU_OID) : null;
				this.unitGooruId = context.containsKey(EventConstants.UNIT_GOORU_OID) ? context.getString(EventConstants.UNIT_GOORU_OID) : null;
				this.courseGooruId = context.containsKey(EventConstants.COURSE_GOORU_OID) ? context.getString(EventConstants.COURSE_GOORU_OID) : null;
				this.classGooruId = context.containsKey(EventConstants.CLASS_GOORU_OID) ? context.getString(EventConstants.CLASS_GOORU_OID) : null;
				this.parentGooruId = context.containsKey(EventConstants.PARENT_GOORU_OID) ? context.getString(EventConstants.PARENT_GOORU_OID) : EventConstants.NA;
				this.parentEventId = context.containsKey(EventConstants.PARENT_EVENT_ID) ? context.getString(EventConstants.PARENT_EVENT_ID) : EventConstants.NA;
				if(context.containsKey(EventConstants.COLLECTION_TYPE) && context.getString(EventConstants.COLLECTION_TYPE).equals(EventConstants.COLLECTION)){
					this.collectionType = EventConstants.COLLECTION;
				} else if (context.containsKey(EventConstants.COLLECTION_TYPE) && context.getString(EventConstants.COLLECTION_TYPE).equals(EventConstants.ASSESSMENT)){
					this.collectionType = EventConstants.ASSESSMENT;
				} else {
					this.collectionType = null;
				}
				this.resourceType = context.containsKey(EventConstants.RESOURCE_TYPE) ? context.getString(EventConstants.RESOURCE_TYPE) : EventConstants.NA;
				this.eventType =  context.containsKey(EventConstants.TYPE) ? context.getString(EventConstants.TYPE) : EventConstants.NA;				
				this.appId = context.containsKey(EventConstants.APP_ID) ? context.getString(EventConstants.APP_ID) : null;				
				this.partnerId = context.containsKey(EventConstants.PARTNER_ID) ? context.getString(EventConstants.PARTNER_ID) : null;				
                this.collectionSubType = context.containsKey(EventConstants.COLLECTION_SUB_TYPE) ? context.getString(EventConstants.COLLECTION_SUB_TYPE) : null;        
				this.pathId = context.containsKey(EventConstants.PATH_ID) ? context.getLong(EventConstants.PATH_ID) : 0L;
                this.sessionId = session.containsKey(EventConstants.SESSION_ID) ? session.getString(EventConstants.SESSION_ID) : EventConstants.NA;        
				this.questionType = payLoadObject.containsKey(EventConstants.QUESTION_TYPE) ? payLoadObject.getString(EventConstants.QUESTION_TYPE) : EventConstants.NA;				
				if(payLoadObject.containsKey(EventConstants.ANSWER_OBECT) && payLoadObject.getValue(EventConstants.ANSWER_OBECT) instanceof JsonArray){
					this.answerObject = payLoadObject.getJsonArray(EventConstants.ANSWER_OBECT);					
				}else{
					this.answerObject = new JsonArray();					
				}				
				this.reportsContext = payLoadObject.containsKey(EventConstants.REPORTS_CONTEXT) ? payLoadObject.getString(EventConstants.REPORTS_CONTEXT) : EventConstants.PERFORMANCE;
				this.gradeType = payLoadObject.containsKey(EventConstants.GRADING_TYPE) ? payLoadObject.getString(EventConstants.GRADING_TYPE) : EventConstants.SYSTEM;
				this.gradeStatus = payLoadObject.containsKey(EventConstants.GRADE_STATUS) ? payLoadObject.getString(EventConstants.GRADE_STATUS) : EventConstants.NA;
				this.teacherId = payLoadObject.containsKey(EventConstants.TEACHER_ID) ? payLoadObject.getString(EventConstants.TEACHER_ID) : EventConstants.NA;
				this.contentFormat = payLoadObject.containsKey(EventConstants.CONTENT_FORMAT) ? payLoadObject.getString(EventConstants.CONTENT_FORMAT) : EventConstants.NA;
	            this.questionCount = context.containsKey(EventConstants.QUESTION_COUNT) ? context.getInteger(EventConstants.QUESTION_COUNT) : 0;         
				if(payLoadObject.containsKey(EventConstants.TAXONOMYIDS) && payLoadObject.getValue(EventConstants.TAXONOMYIDS) instanceof JsonObject){
					this.taxonomyIds = payLoadObject.getJsonObject(EventConstants.TAXONOMYIDS);
				}else{
					this.taxonomyIds = new JsonObject();
				}
				
				this.setStudent(
					(!payLoadObject.containsKey(EventConstants.IS_STUDENT)) || payLoadObject.getBoolean(EventConstants.IS_STUDENT));
				this.eventTime = this.event.getLong(EventConstants.END_TIME);
				this.collectionItemId = EventConstants.NA;
				this.score = metrics.containsKey(EventConstants.SCORE) ? metrics.getDouble(EventConstants.SCORE) :  0;
          // FIXME : Sometime event has string value in reactionType attribute so
          // re-framing this code to accept both numeric and string value
          if (context.containsKey(EventConstants.REACTION_TYPE)) {
            this.reaction = Integer.parseInt(context.getValue(EventConstants.REACTION_TYPE).toString());
          } else {
            this.reaction = 0;
          }
				this.contentSource = context.containsKey(EventConstants.CONTENT_SOURCE) ? context.getString(EventConstants.CONTENT_SOURCE) : null;
								
				this.timezone = this.event.containsKey(EventConstants.TIMEZONE) ? event.getString(EventConstants.TIMEZONE) : null;

				this.sourceId = (context.containsKey("source") ? context.getString("source") : null);
				this.accessToken = (this.session.containsKey("sessionToken") ? this.session.getString("sessionToken") : null);
				if (this.eventName.equals(EventConstants.COLLECTION_PLAY)){
					LOGGER.debug("Inside Collection.Play");
					processCollectionPlayEvents();
				} else if (this.eventName.equals(EventConstants.COLLECTION_RESOURCE_PLAY)){
					LOGGER.debug("Inside Collection.Resourse.Play");
					processCollectionResourcePlayEvents();
				}
				LOGGER.debug("views : {} - timespent : {}", this.views,this.timespent);
			} catch (Exception e) {
				LOGGER.error("Error in event parser : {}", e);
			}			
			return this;
		}
		
        public void processCollectionPlayEvents(){
			LOGGER.debug("collectionType : {} ",this.collectionType);
			if (this.eventType.equals(EventConstants.START) && this.collectionType.equalsIgnoreCase(EventConstants.COLLECTION)) {
				LOGGER.debug("Process Collection.Play Events - Start");
				this.views = 1;
			} else if (this.eventType.equals(EventConstants.STOP)) {
	       LOGGER.debug("Process Collection.Play Events - Stop");
			  if(this.collectionType.equalsIgnoreCase(EventConstants.ASSESSMENT)){
			    this.views = 1;
			  }
				this.timespent = this.endTime - this.startTime;
			}
			
		}
        
        
		public void processCollectionResourcePlayEvents(){
			
			if (this.eventType.equals(EventConstants.START)){
				LOGGER.debug("Process Collection.Resource.Play Events - Start");
				this.views = 1; 
			} else if (this.eventType.equals(EventConstants.STOP)) {
	       LOGGER.debug("Process Collection.Resource.Play Events - Stop");
				this.timespent = this.endTime - this.startTime;			
				if (EventConstants.QUESTION.equals(resourceType)) {
					this.answerStatus = payLoadObject.containsKey(EventConstants.ATTEMPT_STATUS) ? 
							payLoadObject.getString(EventConstants.ATTEMPT_STATUS) : EventConstants.ATTEMPTED;
					
					//Score - EVALUATED Case is not considered at this point
					if (answerStatus.equalsIgnoreCase(EventConstants.INCORRECT) ||
							answerStatus.equalsIgnoreCase(EventConstants.SKIPPED)) {
						this.score = 0;
					} else if (answerStatus.equalsIgnoreCase(EventConstants.CORRECT)) {
						this.score = 1;
					}
				}else{
				  this.answerStatus = EventConstants.UNEVALUATED;
				}				
								
			}
		}

	} // End Class

    