import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;


public class A4Application {

	static class StoreKeyVal {
		Long size;
		Long numOccupants;

		public StoreKeyVal (Long x, Long y) {
			this.numOccupants = x == null ? 0L : x;
			this.size = y == null ? Long.MAX_VALUE : y;
		}
	}

    public static void main(String[] args) throws Exception {
	// do not modify the structure of the command line
		String bootstrapServers = args[0];
		String appName = args[1];
		String studentTopic = args[2];
		String classroomTopic = args[3];
		String outputTopic = args[4];
		String stateStoreDir = args[5];

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir);

	// add code here if you need any additional configuration options

        StreamsBuilder builder = new StreamsBuilder();

		// add code here
		// 
		KStream<String, String> students = builder.stream(studentTopic);
		KStream<String, String> classrooms = builder.stream(classroomTopic);

		KGroupedStream<String, String> studentGStream = students.groupByKey();
		KGroupedStream<String, String> classroomGStream = classrooms.groupByKey();

		KTable<String, Long> totalCapacity = classroomGStream
					.reduce((k, v) -> v)
					.mapValues(k -> Long.parseLong(k));

		KTable<String, Long> occupied = studentGStream
					.reduce((key, value) -> value)
					.groupBy((studentID, roomID) -> KeyValue.pair(roomID.toString(), studentID))
					.count();

		// Build change stream
		
		KStream<String, StoreKeyVal> studentChangeLogs = occupied.toStream()
				.leftJoin(totalCapacity, (numStudents, size) -> new StoreKeyVal(numStudents, size));

		KStream<String, StoreKeyVal> roomChangeLogs = totalCapacity.toStream()
				.leftJoin(occupied, (size, numStudents) -> new StoreKeyVal(numStudents, size));
		
		KStream<String, StoreKeyVal> overallChangeStream = studentChangeLogs.merge(roomChangeLogs);
		
		KTable<String, Long> limitExceeded =
			overallChangeStream.map((k, v) -> KeyValue.pair(k, v.numOccupants - v.size))
							.groupByKey(Serialized.with(Serdes.String(), Serdes.Long()))
							.reduce((key, value) -> value);

		KStream<String, String> result =
			overallChangeStream.join(limitExceeded, (changes, overflow) -> KeyValue.pair(changes, overflow))
							.filter((k, v) -> {
								StoreKeyVal changeLog = v.key;
								return changeLog.numOccupants > changeLog.size || (changeLog.numOccupants == changeLog.size && v.value > 0L);
							})
							.map((k, v) -> {
								StoreKeyVal skv = v.key;
								String toPrint = skv.numOccupants == skv.size ? "OK" : String.valueOf(skv.numOccupants);
								return KeyValue.pair(k, toPrint);
							});
		
		result.to(outputTopic);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

		// this line initiates processing
		streams.start();

		// shutdown hook for Ctrl+C
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
