package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * 已测试
 * Created by AI on 2017/5/10.
 */
public class StreamCC {

    public static void main(String[] args) {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "topicAPCC-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "132.228.28.190:9092,132.228.28.191:9092,132.228.28.192:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //Directory location for state store.
        props.put(StreamsConfig.STATE_DIR_CONFIG,"/home/jsdxadm/wwei/kafka-streams");
        props.put("num.stream.threads",6);
        StreamsConfig config = new StreamsConfig(props);

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> s = builder.stream("topicCCBDR");
        s.mapValues(new ValueMapper<String, String>() {
            @Override
            public String apply(String s) {
                String [] values = s.toString().split(",", -1);
                if(values.length != Constants.CC_ALL_COUNT){
                    return "";
                }
                StringBuffer sb = new StringBuffer();
                sb.append(values[Constants.VOC_CALLTYPE]);
                sb.append(",");
                sb.append(values[Constants.VOC_CALLING]);
                sb.append(",");
                sb.append(values[Constants.VOC_CALLED]);
                sb.append(",");
                sb.append(values[Constants.VOC_START_LAC]);
                sb.append(",");
                sb.append(values[Constants.VOC_START_CI]);
                sb.append(",");
                sb.append(values[Constants.VOC_END_LAC]);
                sb.append(",");
                sb.append(values[Constants.VOC_END_CI]);
                sb.append(",");
                sb.append(values[Constants.VOC_DROP_TYPE]);
                sb.append(",");
                sb.append(values[Constants.VOC_TAKETIME]);
                sb.append(",");
                sb.append(values[Constants.VOC_ORIGIN_PROV_ID]);
                sb.append(",");
                sb.append(values[Constants.VOC_ORIGIN_CITY_ID]);
                sb.append(",");
                sb.append(values[Constants.VOC_DEST_PROV_ID]);
                sb.append(",");
                sb.append(values[Constants.VOC_DEST_CITY_ID]);
                return sb.toString();
            }
        }).to("topicAPCC");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }
}






























