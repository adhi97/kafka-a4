import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Serialized;

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
    // ... = builder.stream(studentTopic);
    // ... = builder.stream(classroomTopic);
    // ...
    // ...to(outputTopic);

    KStream<String, String> studentStream = builder.stream(studentTopic);
    KStream<String, String> classroomStream = builder.stream(classroomTopic);

    KTable<String, Long> capacityTable =
      classroomStream.groupByKey()
                     .reduce((x, y) -> y)
                     .mapValues(x -> Long.parseLong(x));

    KTable<String, Long> occupancyTable =
      studentStream.groupByKey()
                   .reduce((x, y) -> y)
                   .groupBy((studentId, roomId) -> KeyValue.pair(roomId.toString(), studentId))
                   .count();

    KStream<String, RoomInfo> occupancyChangeStream = occupancyTable.toStream().leftJoin(capacityTable,
      (occupants, capacity) -> new RoomInfo(occupants, capacity));

    KStream<String, RoomInfo> capacityChangeStream = capacityTable.toStream().leftJoin(occupancyTable,
      (capacity, occupants) -> new RoomInfo(occupants, capacity));

    KStream<String, RoomInfo> changeInfoStream = occupancyChangeStream.merge(capacityChangeStream);


    KTable<String, Long> roomOverflow =
      changeInfoStream.map((k, v) -> KeyValue.pair(k, v.getDifference()))
                      .groupByKey(Serialized.with(Serdes.String(), Serdes.Long()))
                      .reduce((x, y) -> y);

    KStream<String, String> result =
      changeInfoStream.join(roomOverflow, (changeInfo, numOverflowing) -> new OverflowInfo(changeInfo, numOverflowing))
                      .filter((k, v) -> {
                        RoomInfo rmInfo = v.getRoomInfo();
                        return rmInfo.isOverflowing() || (rmInfo.isAtCapacity() && v.getAmount() > 0L);
                      })
                      .map((k, v) -> {
                        RoomInfo rmInfo = v.getRoomInfo();
                        String toPrint = rmInfo.isAtCapacity() ? "OK" : rmInfo.getOccupants();
                        return KeyValue.pair(k, toPrint);
                      });

    result.to(outputTopic);

    KafkaStreams streams = new KafkaStreams(builder.build(), props);

    // this line initiates processing
    streams.start();

    // shutdown hook for Ctrl+C
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  static class OverflowInfo {
    RoomInfo rmInfo;
    Long amount;

    OverflowInfo(RoomInfo rmInfo, Long amount) {
      this.rmInfo = rmInfo;
      this.amount = amount;
    }

    public Long getAmount() {
      return this.amount;
    }

    public RoomInfo getRoomInfo() {
      return this.rmInfo;
    }

    public String toString() {
      String temp = this.rmInfo.toString();
      return temp + " -- " + this.amount;
    }
  }

  static class RoomInfo {
    Long capacity;
    Long occupants;

    RoomInfo(Long occupying, Long capacity) {
      this.occupants = occupying == null ? 0L : occupying;
      this.capacity = capacity == null ? Long.MAX_VALUE : capacity;
    }

    public long getDifference() {
      return this.occupants - this.capacity;
    }

    public String getOccupants() {
      return String.valueOf(this.occupants);
    }

    public boolean isOverflowing() {
      return this.occupants > this.capacity;
    }

    public boolean isAtCapacity() {
      return this.occupants == this.capacity;
    }

    public String toString() {
      if (this.capacity == Long.MAX_VALUE) {
        return this.occupants + " infinite";
      }
      return this.occupants + " " + this.capacity;
    }
  }
}
