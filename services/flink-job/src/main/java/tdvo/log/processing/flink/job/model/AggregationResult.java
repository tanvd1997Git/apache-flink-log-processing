package tdvo.log.processing.flink.job.model;

import lombok.*;
import org.apache.flink.api.common.typeinfo.*;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@TypeInfo(AggregationResult.AggregationResultTypeInfoFactory.class)
public class AggregationResult {
	Map<String, Integer> ipToErrorRequestCount;
	long processedTime;
	long createdTime;

	public static class AggregationResultTypeInfoFactory extends TypeInfoFactory<AggregationResult> {
		@Override
		public TypeInformation<AggregationResult> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
			Map<String, TypeInformation<?>> fields =
					new HashMap<String, TypeInformation<?>>() {
						{
							put("ipToErrorRequestCount", Types.MAP(Types.STRING, Types.INT));
							put("processedTime", Types.LONG);
							put("createdTime", Types.LONG);
						}
					};
			return Types.POJO(AggregationResult.class, fields);
		}
	}
}
