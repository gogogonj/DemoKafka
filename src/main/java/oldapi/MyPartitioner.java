package oldapi;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Created by AI on 2017/3/2.
 */
public class MyPartitioner implements Partitioner{

    public MyPartitioner(VerifiableProperties verifiableProperties){}

    @Override
    public int partition(Object key, int numPartitions) {
        try {
            return Math.abs(Integer.parseInt((String) key) % numPartitions);
        } catch (Exception e) {
            return Math.abs(key.hashCode() % numPartitions);
        }
    }

}
