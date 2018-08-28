package org.gooru.nucleus.handlers.insights.events.processors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.gooru.nucleus.handlers.insights.events.constants.ConfigConstants;
import org.gooru.nucleus.handlers.insights.events.constants.EventConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.internal.StringUtil;
import io.vertx.core.json.JsonObject;

public class KafkaMessageConsumer implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageConsumer.class);
  private KafkaConsumer<String, String> consumer = null;

  public KafkaMessageConsumer(KafkaConsumer<String, String> consumer) {
    this.consumer = consumer;

  }

  @Override
  public void run() {
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(200);
      for (ConsumerRecord<String, String> record : records) {
        switch (record.topic().split(ConfigConstants.HYPHEN)[0]) {
        case ConfigConstants.KAFKA_EVENTLOGS_TOPIC:
          sendMessage(record.value());
          break;
        case ConfigConstants.KAFKA_TEST_TOPIC:
          LOGGER.info("Test Kafka Consumer : {}", record.value());
          break;
        default:
          // FIXME: Revisit this logic.          
          sendMessage(record.value());
        }
      }
    }
  }

  // Send messages to handlers via event bus
  private void sendMessage(String record) {
    if (!StringUtil.isNullOrEmpty(record)) {
      JsonObject eventObject = null;
      try {
        eventObject = new JsonObject(record);
      } catch (Exception e) {
        LOGGER.warn("Kafka Message should be JsonObject");
      }
      if (eventObject != null) {
        LOGGER.info("RECEIVED EVENT OBJECT :::: {}", eventObject);
        if (eventObject.containsKey(ConfigConstants._EVENT_NAME)
                && eventObject.getString(ConfigConstants._EVENT_NAME).equalsIgnoreCase(EventConstants.RESOURCE_RUBRIC_GRADE)) {
          ProcessorBuilder.buildRubrics(eventObject).process();
        } else {          
          ProcessorBuilder.build(eventObject).process();
        }

      }
    } else {
      LOGGER.warn("NULL or Empty message can not be processed...");
    }
  }
}
