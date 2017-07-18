package org.gooru.nucleus.handlers.insights.events.bootstrap;

import org.gooru.nucleus.handlers.insights.events.rubrics.processors.RubricProcessorBuilder;
import org.gooru.nucleus.handlers.insights.events.rubrics.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.constants.MessagebusEndpoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;

public class GradingWriteVerticle extends AbstractVerticle {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(GradingWriteVerticle.class);

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        EventBus eb = vertx.eventBus();
        
        eb.consumer(MessagebusEndpoints.MBEP_RUBRIC_GRADING_WRITE, message -> {

            LOGGER.debug("Received message '{}'", message.body());

            vertx.executeBlocking(future -> {
                MessageResponse result = RubricProcessorBuilder.build(message).process();
                future.complete(result);
              }, res -> {
//                  MessageResponse result = (MessageResponse) res.result();
//                  message.reply(result.reply(), result.deliveryOptions());
            	  
            	  //No message to be relayed from here.
            });

        }).completionHandler(result -> {
            if (result.succeeded()) {
                LOGGER.info("Grading Write verticle ready to listen");
                startFuture.complete();
            } else {
                LOGGER.error("Grading Write verticle failed to be registered as handler");
                startFuture.fail(result.cause());
            }
        });
}
    
    @Override
    public void stop(Future<Void> startFuture) throws Exception {
      startFuture.complete();
    }

}

    