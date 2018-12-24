package org.gooru.nucleus.handlers.insights.events.bootstrap;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.gooru.nucleus.handlers.insights.events.constants.MessagebusEndpoints;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ashish.
 */

public class PostProcessingVerticle extends AbstractVerticle {


  private static final Logger LOGGER = LoggerFactory
      .getLogger(PostProcessingVerticle.class);

  @Override
  public void start(Future<Void> voidFuture) {
    final EventBus eb = vertx.eventBus();
    eb.<JsonObject>consumer(MessagebusEndpoints.MBEP_POSTPROCESSOR, message -> {
      vertx.<Void>executeBlocking(future -> {
        LOGGER.debug("Post processing event : {}", message.body());
        try {
          ProcessorBuilder.buildPostProcessor(message).process();
          future.complete();
        } catch (Throwable throwable) {
          future.fail(throwable);
        }
      }, res -> {
        if (res.failed()) {
          LOGGER.warn("Failed to update: '{}'", message.body(), res.cause());
        } else {
          LOGGER.debug("Updated event: '{}'");
        }
      });
    }).completionHandler(result -> {
      if (result.succeeded()) {
        voidFuture.complete();
        LOGGER.info("Post processing end point ready to listen");
      } else {
        LOGGER.error(
            "Error registering post processing end point. Halting the Class Reporting machinery");
        voidFuture.fail(result.cause());
        Runtime.getRuntime().halt(1);
      }
    });
  }

  @Override
  public void stop(Future<Void> voidFuture) {
    voidFuture.complete();
  }
}
