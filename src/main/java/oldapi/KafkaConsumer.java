package oldapi;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.Consumer;

/**
 * Created by AI on 2017/2/24.
 */
public class KafkaConsumer extends Thread {

    private final ConsumerConnector consumer;
    private final String topic;

    public KafkaConsumer(String topic) {
        Properties props = new Properties();
        props.put("zookeeper.connect", "127.0.0.1:2181");
        props.put("group.id", "test");
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        ConsumerConfig config = new ConsumerConfig(props);
        consumer = Consumer.createJavaConsumerConnector(config);
        this.topic = topic;
    }

    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, new Integer(1));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        while (it.hasNext()) {
            System.out.println("receive：" + new String(it.next().message()));
            try {
                sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
