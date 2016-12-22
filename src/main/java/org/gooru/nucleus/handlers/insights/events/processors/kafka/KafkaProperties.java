package org.gooru.nucleus.handlers.insights.events.processors.kafka;

import io.vertx.core.json.JsonObject;

public class KafkaProperties {
	
	private JsonObject KafkaProps;
	
	public static final String GROUP_ID = "group.id";
	public static final String KEY_DESERIALIZER = "key.deserializer";
	public static final String VALUE_DESERIALIZER = "value.deserializer";
	public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
	
	public KafkaProperties() {
		
		KafkaProps.put(BOOTSTRAP_SERVERS, "broker1:9092,broker2:9092")
		.put(GROUP_ID, "nucleus")
		.put(KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer")
		.put(VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");		
	}
	
	public void setKafkaProps(JsonObject props) {		
		this.KafkaProps = props;
	}
	
	public JsonObject getKafkaProps() {
		return this.KafkaProps;
	}
}
