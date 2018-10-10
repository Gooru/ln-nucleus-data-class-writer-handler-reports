package org.gooru.nucleus.handlers.insights.events.constants;

public class ConfigConstants {

  //Kafka Config Constants
  public static final String CONFIG_KAFKA_CONSUMER = "defaultKafkaConsumerSettings";
  public static final String CONFIG_KAFKA_SERVERS = "bootstrap.servers";
  public static final String CONFIG_KAFKA_GROUP = "group.id";
  public static final String CONFIG_KAFKA_TOPICS = "consumer.topics";
  public static final String CONFIG_KAFKA_TIME_OUT_IN_MS = "session.timeout.ms";
  public static final String CONFIG_KAFKA_KEY_DESERIALIZER = "key.deserializer";
  public static final String CONFIG_KAFKA_VALUE_DESERIALIZER = "value.deserializer";
  //Expected Kafka Topics
  public static final String KAFKA_EVENTLOGS_TOPIC = "EVENTLOGS";
  public static final String KAFKA_TEST_TOPIC = "test";
  public static final String KAFKA_XAPITRANSFORMER_TOPIC = "XAPITRANSFORMER";
  public static final String KAFKA_RDA_EVENTLOGS_TOPIC = "report.data.aggregate.events";
  
  public static final String THREAD_POOL_SIZE ="thead.pool.size";


  public static final String HYPHEN = "-";
  public static final String COMMA = ",";
  public static final String COLON = ":";

  public static final String EVENT_NAME = "eventName";
  public static final String _EVENT_NAME = "event_name";
}
