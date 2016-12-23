package org.gooru.nucleus.handlers.insights.events.bootstrap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.gooru.nucleus.handlers.insights.events.processors.kafka.KafkaMessageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class MessageConsumerVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumerVerticle.class);

  private static ExecutorService service = null;

  private static ConsumerConnector consumer = null;

  @Override
  public void start(Future<Void> voidFuture) throws Exception {
    JsonObject config = config();
    LOGGER.debug("config: " + config);
    vertx.executeBlocking(blockingFuture -> {
      createConsumer();
      blockingFuture.complete();
    }, startApplicationFuture -> {
      if (startApplicationFuture.succeeded()) {
        vertx.executeBlocking(future -> {
          service = Executors.newFixedThreadPool(10);
          consumeMessages();
          future.complete();
        }, res -> {
          // No message to be relayed from here.
        });
      } else {
        voidFuture.fail("Not able to initialize the verticle machinery properly");
      }
    });

  }

  @Override
  public void stop(Future<Void> voidFuture) throws Exception {
    consumer.shutdown();
  }

  private void createConsumer() {
    JsonObject config = config();
    Properties props = new Properties();
    props.setProperty(MessageConstants.CONFIG_ZK_CONNECT, config.getString(MessageConstants.CONFIG_ZK_CONNECT));
    props.setProperty(MessageConstants.CONFIG_ZK_GROUP, config.getString(MessageConstants.CONFIG_ZK_GROUP));
    props.setProperty(MessageConstants.CONFIG_ZK_TIME_OUT, config.getString(MessageConstants.CONFIG_ZK_TIME_OUT));
    props.setProperty(MessageConstants.CONFIG_ZK_SYNC_TIME, config.getString(MessageConstants.CONFIG_ZK_SYNC_TIME));
    props.setProperty(MessageConstants.CONFIG_ZK_COMMIT_INTERVAL, config.getString(MessageConstants.CONFIG_ZK_COMMIT_INTERVAL));
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
  }

  private void consumeMessages() {
    String[] topics = config().getString(MessageConstants.CONFIG_ZK_TOPIC).split(",");
    Map<String, Integer> topicCountMap = new HashMap<>();
    for (final String consumerTopic : topics) {
      topicCountMap.put(consumerTopic, 1);
    }
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    for (final String consumerTopic : topics) {
      LOGGER.debug("Consumer topic : " + consumerTopic);
      service.submit(new KafkaMessageConsumer(consumerTopic, consumerMap) {
      });
    }
  }
}
