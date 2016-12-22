package org.gooru.nucleus.handlers.insights.events.bootstrap;

import org.gooru.nucleus.handlers.insights.events.bootstrap.shutdown.Finalizer;
import org.gooru.nucleus.handlers.insights.events.bootstrap.shutdown.Finalizers;
import org.gooru.nucleus.handlers.insights.events.bootstrap.startup.Initializer;
import org.gooru.nucleus.handlers.insights.events.bootstrap.startup.Initializers;
import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.gooru.nucleus.handlers.insights.events.constants.MessagebusEndpoints;
import org.gooru.nucleus.handlers.insights.events.processors.ProcessorBuilder;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;


public class InsightsWriteVerticle extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsightsWriteVerticle.class);

    @Override
    public void start(Future<Void> voidFuture) throws Exception {
        EventBus eb = vertx.eventBus();

        vertx.executeBlocking(blockingFuture -> {
            startApplication();
            blockingFuture.complete();
        }, startApplicationFuture -> {
            if (startApplicationFuture.succeeded()) {
                eb.consumer(MessagebusEndpoints.MBEP_ANALYTICS_WRITE, message -> {
                    vertx.executeBlocking(future -> {
                        LOGGER.info("Received message");
                        MessageResponse result = ProcessorBuilder.build(message).process();                    	
                        future.complete(result);
                    }, res -> {
                    	//No message to be relayed from here.
                    });
                }).completionHandler(result -> {
                    if (result.succeeded()) {
                        voidFuture.complete();
                        LOGGER.info("Class Reporting end point ready to listen");
                    } else {
                        LOGGER.error("Error registering the assessment handler. Halting the Class Reporting machinery");
                        voidFuture.fail(result.cause());
                        Runtime.getRuntime().halt(1);
                    }
                });
            } else {
                voidFuture.fail("Not able to initialize the verticle machinery properly");
            }
        });

    }

    @Override
    public void stop() throws Exception {
        shutDownApplication();
        super.stop();
    }

    private void startApplication() {
        Initializers initializers = new Initializers();
        try {
            for (Initializer initializer : initializers) {
                initializer.initializeComponent(vertx, config());
            }
        } catch (IllegalStateException ie) {
            LOGGER.error("Error initializing application", ie);
            Runtime.getRuntime().halt(1);
        }
    }

    private void shutDownApplication() {
        Finalizers finalizers = new Finalizers();
        for (Finalizer finalizer : finalizers) {
            finalizer.finalizeComponent();
        }

    }

}
