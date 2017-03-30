package oldapi;

/**
 * Created by AI on 2017/2/24.
 */
public class KafkaConsumerProducerDemo {
    public static void main(String[] args) {
        //producer
        KafkaProducer producerThread = new KafkaProducer("topic1");
        producerThread.start();

        //consumer
        KafkaConsumer consumerThread = new KafkaConsumer("topic1");
        consumerThread.start();
    }
}
