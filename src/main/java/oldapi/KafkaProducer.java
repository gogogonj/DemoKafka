package oldapi;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by AI on 2017/2/24.
 */
public class KafkaProducer extends Thread {
    private Producer<Integer, String> producer;
    private String topic;

    public KafkaProducer(String topic) {
        Properties props = new Properties();
        //需要给出message的序列化的encoder
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //给出一些broker地址，不用全部，这里建议给两个防止一个不可用
        props.put("metadata.broker.list", "127.0.0.1:9092");
        //可以不设置，默认就是random partition，当然这里可以自定义，如何根据key来选择partition
        props.put("partitioner.class","com.asiainfo.kafka.oldapi.MyPartitioner");

        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<>(config);

        this.topic = topic;
    }

    @Override
    public void run() {
        int messageNo = 1;
        while (true) {

            //向指定topic发送消息
            producer.send(new KeyedMessage<Integer, String>(topic, new String("Message_" + messageNo)));

            messageNo++;
            try {
                sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}