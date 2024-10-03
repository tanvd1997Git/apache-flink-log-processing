package tdvo.log.generator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;

public class Main {
	public static void main(String[] args) {
		long totalRecords = Long.parseLong(args[0]);
		int targetThroughput = Integer.parseInt(args[1]);

		ThroughputThrottler throttler = new ThroughputThrottler(targetThroughput, Instant.now().toEpochMilli());
		KafkaProducer<String, String> kafkaProducer = configureProducer();
		String TARGET_TOPIC = "log-event";
		for (long i = 0; i < totalRecords; i++) {
			ProducerRecord<String, String> record = new ProducerRecord<>(TARGET_TOPIC, NGINXLogGenerator.generate());
			long sendStartMs = Instant.now().toEpochMilli();
			kafkaProducer.send(record);
			if (throttler.shouldThrottle(i, sendStartMs)) {
				throttler.throttle();
			}
		}
		kafkaProducer.close();
	}

	private static KafkaProducer<String, String> configureProducer() {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:29092");
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());
		return new KafkaProducer<>(properties);
	}
}