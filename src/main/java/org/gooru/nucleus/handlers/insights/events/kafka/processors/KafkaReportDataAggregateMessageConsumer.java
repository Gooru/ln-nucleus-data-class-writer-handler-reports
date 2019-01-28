package org.gooru.nucleus.handlers.insights.events.kafka.processors;

import io.netty.util.internal.StringUtil;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.gooru.nucleus.handlers.insights.events.constants.ConfigConstants;
import org.gooru.nucleus.handlers.insights.events.processors.RDAMessageProcessorBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author renuka@gooru
 */
public class KafkaReportDataAggregateMessageConsumer implements Runnable {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(KafkaReportDataAggregateMessageConsumer.class);
  private KafkaConsumer<String, String> consumer = null;

  public KafkaReportDataAggregateMessageConsumer(KafkaConsumer<String, String> consumer) {
    this.consumer = consumer;

  }

  @Override
  public void run() {
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(200);
      for (ConsumerRecord<String, String> record : records) {
        switch (record.topic().split(ConfigConstants.HYPHEN)[0]) {
          case ConfigConstants.KAFKA_RDA_EVENTLOGS_TOPIC:
            sendMessage(record.value());
            break;
          case ConfigConstants.KAFKA_TEST_TOPIC:
            LOGGER.info("Test Kafka Consumer : {}", record.value());
            break;
          default:
            // Do nothing
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
        LOGGER.info("RECEIVED RDA EVENT OBJECT :::: {}", eventObject);
        RDAMessageProcessorBuilder.buildKafkaProcessor(eventObject).process();
      }
    } else {
      LOGGER.warn("NULL or Empty message can not be processed...");
    }
  }
}
