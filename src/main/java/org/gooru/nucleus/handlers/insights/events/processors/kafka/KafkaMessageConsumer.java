package org.gooru.nucleus.handlers.insights.events.processors.kafka;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.insights.events.processors.ProcessorBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public abstract class KafkaMessageConsumer implements Runnable {

  private final Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap;

  private final String consumerTopic;

  private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageConsumer.class);

  public KafkaMessageConsumer(String consumerTopic, Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap) {
    this.consumerTopic = consumerTopic;
    this.consumerMap = consumerMap;
  }

  @Override
  public void run() {
    try {
      KafkaStream<byte[], byte[]> stream = consumerMap.get(consumerTopic).get(0);
      for (MessageAndMetadata<byte[], byte[]> aStream : stream) {
        String message = new String(aStream.message());
        LOG.info("Consuming event: {}", message);
        if (!StringUtil.isNullOrEmpty(message)) {
          ProcessorBuilder.build(message).process();
        } else {
          LOG.warn("NULL or Empty message can not be processed...");
        }
      }
    } catch (Exception e) {
      LOG.error("Error while consume messages : {}", e);
    }
  }
}
