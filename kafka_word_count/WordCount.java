import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class WordCount {

	public static void main(String[] args) throws Exception {

		String bootstrapServers = args[0];
		String inputTopic = args[1];
		String outputTopic = args[2];
		String applicationId = args[3];

		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> textLines = builder.stream(inputTopic);
		KTable<String, Long> wordCounts = textLines
			// Split each text line, by whitespace, into words.
			.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
			// Originally key was set to null, here we need to replace it with the value.
			.map((key, value) -> new KeyValue<String, String>(value, value))
			.groupByKey()
			// Count the occurrences of each word (message key).
			.count();

		final Serde<String> stringSerde = Serdes.String();
		final Serde<Long> longSerde = Serdes.Long();
		// Store the running counts as a changelog stream to the output topic.
		wordCounts.toStream().to(outputTopic, Produced.with(stringSerde, longSerde));

		KafkaStreams streams = new KafkaStreams(builder.build(), config);
		// this line initiates processing
		streams.start();

		// shutdown hook for Ctrl+C
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}