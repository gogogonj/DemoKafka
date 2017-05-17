package newapi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class MyConsumer {
    public static void main(String[] s){

        KafkaConsumer<String, String> consumer = KafkaUtil.getConsumer();
        consumer.subscribe(Arrays.asList("replication-test"));
        consumer.seekToBeginning(new ArrayList<TopicPartition>());
        //consumer.seekToEnd(new ArrayList<TopicPartition>());
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for(ConsumerRecord<String, String> record : records) {
                System.out.println("fetched from partition " + record.partition() + ", offset: " + record.offset() + ", message: " + record.value());
            }
            //按分区读取数据
//              for (TopicPartition partition : records.partitions()) {
//                  List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
//                  for (ConsumerRecord<String, String> record : partitionRecords) {
//                      System.out.println(record.offset() + ": " + record.value());
//                  }
//              }

        }

    }
}
