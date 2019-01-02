package org.gooru.nucleus.handlers.insights.events.bootstrap;

import org.gooru.nucleus.handlers.insights.events.constants.ConfigConstants;
import org.gooru.nucleus.handlers.insights.events.constants.MessageConstants;
import org.gooru.nucleus.handlers.insights.events.constants.MessagebusEndpoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;

/**
 * @author mukul@gooru
 */
public class EBSendVerticle extends AbstractVerticle {

	private final Logger LOGGER = LoggerFactory.getLogger(EBSendVerticle.class);
	private static EventBus eb = null;
	private static JsonObject conf;

	@Override
	public void start() {
		LOGGER.info("EBSendVerticle Started");
		eb = vertx.eventBus();
		conf = config().getJsonObject("config");
	}

	public void sendMessage(JsonObject event) {		
		DeliveryOptions options = new DeliveryOptions()
				.setSendTimeout(conf.getLong(ConfigConstants.MBUS_TIMEOUT, 30L) * 1000)
				.addHeader(MessageConstants.MSG_HEADER_OP, MessageConstants.MSG_OP_DIAGNOSTIC_ASMT);
		eb.send(MessagebusEndpoints.MBEP_POSTPROCESSOR, event, options);
	}

	@Override
	public void stop(Future<Void> voidFuture) {
		voidFuture.complete();
	}
}
