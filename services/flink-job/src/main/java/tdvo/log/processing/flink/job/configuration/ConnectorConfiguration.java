package tdvo.log.processing.flink.job.configuration;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ConnectorConfiguration {

	public static KafkaSource<String> configureLogEventKafkaSource(ParameterTool properties) {
		return KafkaSource.<String>builder()
				.setBootstrapServers(properties.get("kafka.bootstrap.servers"))
				.setTopics(properties.get("kafka.log.event.topic.name"))
				.setGroupId("log-event-kafka-source")
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
				.build();
	}

	public static KafkaSink<String> configureKafkaSink(ParameterTool params) {
		return KafkaSink.<String>builder().setBootstrapServers(params.get("kafka.bootstrap.servers"))
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
								.setTopic(params.get("kafka.log.processing.output.topic.name"))
								.setValueSerializationSchema(new SimpleStringSchema())
								.build())
				.build();

	}
}
