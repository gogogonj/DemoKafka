package newapi;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * Created by AI on 2017/3/17.
 */
public class KafkaUtil {
    private static KafkaProducer<String, String> kp;
    private static KafkaConsumer<String, String> kc;

    public static KafkaProducer<String, String> getProducer() {
        if (kp == null) {
            Properties props = new Properties();
            System.setProperty("java.security.auth.login.config", "/home/wangwei/opt/test/kafka_client_jaas_admin.conf"); // 环境变量添加，需要输入配置文件的路径
            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("sasl.mechanism", "PLAIN");

            props.put("bootstrap.servers", "127.0.0.1:9092");
            props.put("acks", "1");
            props.put("retries", 1);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            kp = new KafkaProducer<String, String>(props);
        }
        return kp;
    }


    public static KafkaConsumer<String, String> getConsumer() {
        if(kc == null) {
            Properties props = new Properties();
            System.setProperty("java.security.auth.login.config", "/home/wangwei/opt/test/kafka_client_jaas_alice.conf"); // 环境变量添加，需要输入配置文件的路径
            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("sasl.mechanism", "PLAIN");

            props.put("bootstrap.servers", "127.0.0.1:9092");
            props.put("group.id", "test");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            kc = new KafkaConsumer<String, String>(props);
        }
        return kc;
    }
}
