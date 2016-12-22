package org.gooru.nucleus.handlers.insights.events.processors.kafka;

public abstract class KafkaConsumer implements Runnable {
	
/**	KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(new KafkaProperties());

	@Override
	public void run() {
		try {
		      consumer.subscribe(topics);

		      while (true) {
		        ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
		        for (ConsumerRecord<String, String> record : records) {
		          
		        }
		      }
		    } catch (WakeupException e) {
		      // ignore for shutdown 
		    } finally {
		      consumer.close();
		    }
		  }

		  public void shutdown() {
		    consumer.wakeup();
		  }
		
	} **/
}
