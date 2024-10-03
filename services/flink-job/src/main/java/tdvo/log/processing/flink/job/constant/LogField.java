package tdvo.log.processing.flink.job.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum LogField {
	REMOTE_ADDRESS("remoteAddress"),
	STATUS_CODE("statusCode"),
	CREATED_TIME("createdTime");

	private final String value;
}
