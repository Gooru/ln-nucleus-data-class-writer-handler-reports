package org.gooru.nucleus.handlers.insights.events.bootstrap;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.gooru.nucleus.handlers.insights.events.constants.ConfigConstants;
import org.gooru.nucleus.handlers.insights.events.kafka.KafkaMessageConsumer1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

public class GradingWriteVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(GradingWriteVerticle.class);
  private static KafkaConsumer<String, String> consumer = null;
  private static ExecutorService service = null;

  @Override
  public void start(Future<Void> voidFuture) throws Exception {
    vertx.executeBlocking(blockingFuture -> {
      LOGGER.debug("Config ::" + config());
      service = Executors.newFixedThreadPool(config().getInteger(ConfigConstants.THREAD_POOL_SIZE));
      createConsumer(config().getJsonObject(ConfigConstants.CONFIG_KAFKA_CONSUMER));
      blockingFuture.complete();
    }, startApplicationFuture -> {
      if (!startApplicationFuture.succeeded()) {
        LOGGER.error("Error :{}", startApplicationFuture.cause());
        voidFuture.fail("Not able to initialize the verticle machinery properly");
      }
    });
  }

  @Override
  public void stop(Future<Void> voidFuture) throws Exception {
    consumer.close();
    voidFuture.complete();
  }

  private void createConsumer(JsonObject config) {
    LOGGER.debug("Configuring KAFKA consumer.....");
    Properties props = new Properties();
    props.put(ConfigConstants.CONFIG_KAFKA_SERVERS, config.getString(ConfigConstants.CONFIG_KAFKA_SERVERS));
    props.put(ConfigConstants.CONFIG_KAFKA_TIME_OUT_IN_MS, config.getString(ConfigConstants.CONFIG_KAFKA_TIME_OUT_IN_MS));
    props.put(ConfigConstants.CONFIG_KAFKA_GROUP, config.getString(ConfigConstants.CONFIG_KAFKA_GROUP));
    props.put(ConfigConstants.CONFIG_KAFKA_KEY_DESERIALIZER, StringDeserializer.class.getName());
    props.put(ConfigConstants.CONFIG_KAFKA_VALUE_DESERIALIZER, StringDeserializer.class.getName());
    consumer = new KafkaConsumer<>(props);
    // Topics should be combination of address and environment. Address and
    // environment should be separated by HYPHEN. Address should not contain
    // HYPHEN. EG : EVENTLOGS-QA, TEST-PROD, XAPITRANSFORMER-QA
    String[] topics = config.getString(ConfigConstants.CONFIG_KAFKA_TOPICS).split(ConfigConstants.COMMA);
    consumer.subscribe(Arrays.asList(topics));
    service.submit(new KafkaMessageConsumer1(consumer));
  }
}
