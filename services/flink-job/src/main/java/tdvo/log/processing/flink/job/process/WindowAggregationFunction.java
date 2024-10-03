package tdvo.log.processing.flink.job.process;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.apache.flink.api.common.functions.AggregateFunction;
import tdvo.log.processing.flink.job.constant.LogField;
import tdvo.log.processing.flink.job.model.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;


public class WindowAggregationFunction implements AggregateFunction<String, AggregationAccumulator, AggregationResult> {

	@Override
	public AggregationAccumulator createAccumulator() {
		return new AggregationAccumulator();
	}

	@Override
	public AggregationAccumulator add(String logEvent, AggregationAccumulator aggregationAccumulator) {
		Any logEventAny = JsonIterator.deserialize(logEvent);
		if (aggregationAccumulator.getCreatedTimestamp() == 0) {
			long createdTimestamp = logEventAny.toLong(LogField.CREATED_TIME.getValue());
			long statusCode = logEventAny.toLong(LogField.STATUS_CODE.getValue());
			String ip = logEventAny.toString(LogField.REMOTE_ADDRESS.getValue());
			Map<String, Integer> ipToErrorRequestCount = new HashMap<>();
			ipToErrorRequestCount.put(ip, statusCode >= 400 ? 1 : 0);
			aggregationAccumulator.setCreatedTimestamp(createdTimestamp);
			aggregationAccumulator.setIpToErrorRequestCount(ipToErrorRequestCount);
			return aggregationAccumulator;
		}

		long statusCode = logEventAny.toLong(LogField.STATUS_CODE.getValue());
		String ip = logEventAny.toString(LogField.REMOTE_ADDRESS.getValue());
		Map<String, Integer> ipToErrorRequestCount = aggregationAccumulator.getIpToErrorRequestCount();
		Integer currentErrorCount = ipToErrorRequestCount.getOrDefault(ip, 0);
		ipToErrorRequestCount.put(ip, statusCode >= 400 ? currentErrorCount + 1 : currentErrorCount);
		return aggregationAccumulator;
	}

	@Override
	public AggregationAccumulator merge(AggregationAccumulator acc1, AggregationAccumulator acc2) {
		return null;
	}

	@Override
	public AggregationResult getResult(AggregationAccumulator aggregationAccumulator) {
		return new AggregationResult(aggregationAccumulator.getIpToErrorRequestCount(), Instant.now().toEpochMilli(), aggregationAccumulator.getCreatedTimestamp());
	}
}
