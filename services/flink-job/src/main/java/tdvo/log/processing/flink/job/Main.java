package tdvo.log.processing.flink.job;

import com.jsoniter.output.JsonStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import tdvo.log.processing.flink.job.configuration.ConnectorConfiguration;
import tdvo.log.processing.flink.job.model.*;
import tdvo.log.processing.flink.job.process.WindowAggregationFunction;

@Slf4j
public class Main {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		if (env instanceof LocalStreamEnvironment) {
			env.setParallelism(1);
		}

		final ParameterTool propertiesParameter = ParameterTool.fromPropertiesFile(Main.class.getClassLoader().getResourceAsStream("application.properties"));

		DataStream<String> logEventStream = env.fromSource(
				ConnectorConfiguration.configureLogEventKafkaSource(propertiesParameter),
				WatermarkStrategy.noWatermarks(),
				"Log event Kafka source"
		).name("Log event data stream");

		DataStream<AggregationResult> aggregationResultStream = logEventStream
				.keyBy(logEvent -> "")
				.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
				.aggregate(new WindowAggregationFunction())
				.name("Tumbling window aggregation function");

		DataStream<String> logProcessingOutputStream = aggregationResultStream
				.map(new MapFunction<AggregationResult, String>() {
					@Override
					public String map(AggregationResult aggregationResult) throws Exception {
						return JsonStream.serialize(aggregationResult);
					}
				});

		KafkaSink<String> resultSink = ConnectorConfiguration.configureKafkaSink(propertiesParameter);
		logProcessingOutputStream.sinkTo(resultSink);

		log.info("Job is running...");

		env.disableOperatorChaining();
		env.execute("Log processing job");
	}
}