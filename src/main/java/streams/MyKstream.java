package streams;

import org.apache.kafka.streams.processor.TopologyBuilder;

/**
 * Created by AI on 2017/4/10.
 */
public class MyKstream {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("SOURCE", "replication-test")
                // add "PROCESS1" node which takes the source processor "SOURCE" as its upstream processor
                .addProcessor("PROCESS1", () -> new MyProcessor(), "SOURCE")
                // add the sink processor node "SINK1" that takes Kafka topic "sink-topic1"
                // as output and the "PROCESS1" node as its upstream processor
                .addSink("SINK1", "replication-test2", "PROCESS1");
    }

}
