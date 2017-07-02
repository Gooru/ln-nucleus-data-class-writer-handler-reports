package org.gooru.nucleus.handlers.insights.events.processors;

import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.SerializationException;
import org.gooru.nucleus.handlers.insights.events.app.components.KafkaRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

public final class MessageDispatcher {
  private static final MessageDispatcher INSTANCE = new MessageDispatcher();
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageDispatcher.class);

  private MessageDispatcher() {
  }

  public static MessageDispatcher getInstance() {
    return INSTANCE;
  }

  public void sendMessage2Kafka(JsonObject eventBody) {
    if (KafkaRegistry.getInstance().testWithoutKafkaServer()) {
      return; // running without KafkaServer...
    }
    sendMessageToKafka(KafkaRegistry.getInstance().getKafkaTopic(), eventBody);
  }

  private void sendMessageToKafka(String topic, JsonObject eventBody) {
    Producer<String, String> producer = KafkaRegistry.getInstance().getKafkaProducer();
    ProducerRecord<String, String> kafkaMsg;

    kafkaMsg = new ProducerRecord<>(topic, eventBody.toString());

    try {
      if (producer != null) {
        producer.send(kafkaMsg, (metadata, exception) -> {
          if (exception == null) {
            LOGGER.info("Message Delivered Successfully: Offset : " + metadata.offset() + " : Topic : " + metadata.topic() + " : Partition : "
                    + metadata.partition() + " : Message : " + kafkaMsg);
          } else {
            LOGGER.error("Message Could not be delivered : " + kafkaMsg + ". Cause: " + exception.getMessage());
          }
        });
        LOGGER.debug("Message Sent Successfully: " + kafkaMsg);
      } else {
        LOGGER.error("Not able to obtain producer instance");
      }
    } catch (InterruptException | BufferExhaustedException | SerializationException ie) {
      // - If the thread is interrupted while blocked
      LOGGER.error("sendMessageToKafka: to Kafka server:", ie);
    }
  }
}
