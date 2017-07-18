package org.gooru.nucleus.handlers.insights.events.constants;

public final class MessagebusEndpoints {
    /*
     * Any change here in end points should be done in the gateway side as well,
     * as both sender and receiver should be in sync
     */
		
	//Mukul - Newly added constant for Class Reports
    public static final String MBEP_ANALYTICS_WRITE = "org.gooru.nucleus.message.bus.analytics.write";
    public static final String MBEP_EVENT = "org.gooru.nucleus.message.bus.publisher.event";
    
    //Rubric Message Bus Endpoint
	public static final String MBEP_RUBRIC_GRADING_WRITE = "org.gooru.nucleus.message.bus.rubric.grading.write";


    private MessagebusEndpoints() {
        throw new AssertionError();
    }
}
