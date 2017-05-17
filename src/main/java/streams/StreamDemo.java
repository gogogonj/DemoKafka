package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * 已测试
 * Created by AI on 2017/5/10.
 */
public class StreamDemo {
    // 传入用户的字段，用户需要的topic，用户指定的topic
    public static void main(String[] args) {

        //用户需要的字段
        String need = "a,c,e,g";
        String [] needs = need.split(",");
        //已有的字段，必须和值的顺序对应
        List<String> allList = new ArrayList<String>();
        allList.add("a");
        allList.add("b");
        allList.add("c");
        allList.add("d");
        allList.add("e");
        allList.add("f");

        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "132.228.28.190:9092,132.228.28.191:9092,132.228.28.192:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put("num.stream.threads",2);
        StreamsConfig config = new StreamsConfig(props);

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> s = builder.stream("replication-test");
        s.mapValues(new ValueMapper<String, String>() {
            @Override
            public String apply(String s) {
                String [] values = s.toString().split(",");
                String value = "";
                for(int i = 0; i<needs.length; i++){
                    int num = allList.indexOf(needs[i]);
                    if(num!=-1){
                        value += values[num]+",";
                    }
                }
                return value.substring(0,value.length()-1);
            }
        }).to("replication-test2");

//        builder.stream("replication-test").mapValues(value ->
//
//                    value.toString().split(",")[i]
//
//        ).to("replication-test2");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }
}






























