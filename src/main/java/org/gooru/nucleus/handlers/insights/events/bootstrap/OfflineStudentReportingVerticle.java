
 package org.gooru.nucleus.handlers.insights.events.bootstrap;

 import org.gooru.nucleus.handlers.insights.events.constants.MessagebusEndpoints;
 import org.gooru.nucleus.handlers.insights.events.processors.ProcessorBuilder;
 import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

 import io.vertx.core.AbstractVerticle;
 import io.vertx.core.Future;
 import io.vertx.core.eventbus.EventBus;

 /**
  * @author renuka@gooru
  * 
  */
 public class OfflineStudentReportingVerticle extends AbstractVerticle {
    
     private static final Logger LOGGER = LoggerFactory.getLogger(OfflineStudentReportingVerticle.class);
     @Override
     public void start(Future<Void> voidFuture) throws Exception {
     final EventBus eb = vertx.eventBus();
     eb.consumer(MessagebusEndpoints.MBEP_ANALYTICS_OFFLINE_REPORT, message -> {
       LOGGER.debug("Received message at the Offline Student Reporting Verticle: {}", message.body());
       vertx.executeBlocking(future -> {
         MessageResponse result = ProcessorBuilder.buildOfflineStudentReportingProcessor(message).process();
         future.complete(result);
       }, res -> {
          MessageResponse result = (MessageResponse) res.result();
           message.reply(result.reply(), result.deliveryOptions());
       });

     }).completionHandler(result -> {
       if (result.succeeded()) {
         voidFuture.complete();
         LOGGER.info("Offline Student Reporting end point ready to listen");
       } else {
         LOGGER.error("Error registering the Offline Student Reporting Verticle. Halting the Class Reporting machinery");
         voidFuture.fail(result.cause());
         Runtime.getRuntime().halt(1);
       }
     });
   }
     @Override
     public void stop(Future<Void> voidFuture) throws Exception {
       voidFuture.complete();
     }
 }