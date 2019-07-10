package issues;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import brave.Tracing;
import brave.kafka.streams.KafkaStreamsTracing;

public class TopologySample {

	public static void main(String[] args) {

		System.out.println("default Topology\n" + buildDefaultTopology());

		System.out.println("brave Topology\n" + buildBraveTopology());

	}

	private static TopologyDescription buildDefaultTopology() {
		StreamsBuilder builder = new StreamsBuilder();
		KTable<String, String> table = builder.table("table");
		KStream<String, String> stream = builder.stream("input");

		stream
				.filter((k, v) -> true)
				.join(table, (left, right) -> left)
				.to("output");

		return builder.build().describe();
	}

	private static TopologyDescription buildBraveTopology() {
		KafkaStreamsTracing kafkaStreamsTracing = KafkaStreamsTracing.create(Tracing.newBuilder().build());

		StreamsBuilder builder = new StreamsBuilder();
		KTable<String, String> table = builder.table("table");
		KStream<String, String> stream = builder.stream("input");

		stream
				.filter((k, v) -> true)
				.transform(kafkaStreamsTracing.filter("spanName", (k, v) -> true))
				.join(table, (left, right) -> left)
				.to("output");

		return builder.build().describe();
	}

}
