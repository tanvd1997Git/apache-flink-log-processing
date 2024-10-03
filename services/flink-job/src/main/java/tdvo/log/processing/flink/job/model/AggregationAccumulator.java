package tdvo.log.processing.flink.job.model;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class AggregationAccumulator {
	Map<String, Integer> ipToErrorRequestCount;
	long createdTimestamp;
}
