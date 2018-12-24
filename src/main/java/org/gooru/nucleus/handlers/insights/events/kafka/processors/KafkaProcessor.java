package org.gooru.nucleus.handlers.insights.events.kafka.processors;

import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;

/**
 * @author mukul@gooru
 */
public interface KafkaProcessor {

  MessageResponse process();

}
