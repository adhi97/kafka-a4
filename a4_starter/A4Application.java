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
					.reduce((x, y) -> y)
					.mapValues(x -> Long.parseLong(x));

		KTable<String, Long> occupied = studentGStream
					.reduce((key, value) -> value)
					.groupBy((studentID, roomID) -> KeyValue.pair(roomID.toString(), studentID))
					.count();

		// Build change stream
		
		KStream<String, KeyValue> occupancyChangeStream = occupied.toStream().leftJoin(totalCapacity,
			(occupants, capacity) -> new KeyValue<Long, Long>(occupants, capacity));

		KStream<String, KeyValue> capacityChangeStream = totalCapacity.toStream().leftJoin(occupied,
      		(capacity, occupants) -> new KeyValue<Long, Long>(occupants, capacity));
		
		KStream<String, KeyValue> changeInfoStream = occupancyChangeStream.merge(capacityChangeStream);
		
		KTable<String, Long> roomOverflow =
			changeInfoStream.map((k, v) -> KeyValue.pair(k, v.value - v.key))
							.groupByKey(Serialized.with(Serdes.String(), Serdes.Long()))
							.reduce((key, value) -> value);

		KStream<String, String> result =
			changeInfoStream.join(roomOverflow, (changeInfo, numOverflowing) -> KeyValue.pair(changeInfo, numOverflowing))
							.filter((k, v) -> {
								KeyValue changeLog = v.key;
								return changeLog.value > changeLog.key || (changeLog.key == changeLog.value && v.value > 0L);
							})
							.map((k, v) -> {
								KeyValue rmInfo = v.key;
								String toPrint = rmInfo.key == rmInfo.value ? "OK" : String.valueOf(rmInfo.value);
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
