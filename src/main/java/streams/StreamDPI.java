package streams;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by AI on 2017/6/20.
 */
public class StreamDPI {
    private static KafkaProducer<String, String> producer;
    private static String sTopic = "JSCTGW_144";
    private static String topic = "JSCTGW_144_test";

    public static void main(String[] args) {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "JSCTGW_144-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "132.228.28.144:9092,132.228.28.145:9092,132.228.28.146:9092,132.228.28.147:9092,132.228.28.148:9092,132.228.28.149:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //Directory location for state store.
        props.put(StreamsConfig.STATE_DIR_CONFIG,"/home/jsdxadm/wwei/kafka-streams");
        props.put("num.stream.threads",3);
        StreamsConfig config = new StreamsConfig(props);

        //===================
        Properties props2 = new Properties();
        props2.put("bootstrap.servers", "132.228.28.190:9092,132.228.28.191:9092,132.228.28.192:9092");
        props2.put("acks", "1");
        props2.put("retries", 1);
        props2.put("batch.size", 16384);
        props2.put("linger.ms", 1);
        props2.put("buffer.memory", 33554432);
        props2.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props2.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props2);
        //===================

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> s = builder.stream(sTopic);
        s.mapValues(new ValueMapper<String, String>() {
            @Override
            public String apply(String s) {
                String [] values = s.toString().split("\\|", -1);
                if(values.length < 4){
                    return "";
                }
                StringBuffer sb = new StringBuffer();
                sb.append(values[3]);

                //========发送到外部系统start========
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, sb.toString());
                producer.send(record);
                //========发送到外部系统end==========
                return sb.toString();
            }
        });

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }
}
