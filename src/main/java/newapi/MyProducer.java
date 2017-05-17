package newapi;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MyProducer {
    public static void main(String[] s) {

        try {

            Producer<String, String> producer = KafkaUtil.getProducer();
            int i = 0;
            while(true) {
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("replication-test", String.valueOf(i), "java"+i+",c,js,web");
                producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null)
                            e.printStackTrace();
                        System.out.println("message send to partition " + metadata.partition() + ", offset: " + metadata.offset());
                    }
                });
                i++;
                Thread.sleep(1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
