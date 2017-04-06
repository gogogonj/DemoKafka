package topics;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Created by AI on 2017/4/5.
 */
public class TopicAPITest {
    public static void main(String [] args){
        if(args.length!=1){
            System.out.println("缺少参数=========");
            return;
        }
        String type = args[0];
        if("1".equals(type)){
            createTopic();
        }else if("2".equals(type)){
            deleteTopic();
        }else if("3".equals(type)){
            queryTopic();
        }
    }

    /**
     * 创建topic
     */
    public static void createTopic(){
        ZkUtils zkUtils = ZkUtils.apply("localhost:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
        // 创建一个单分区单副本名为t1的topic
        AdminUtils.createTopic(zkUtils, "t1", 1, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
        zkUtils.close();
    }

    /**
     * 删除topic
     * 测试时好像无效！
     */
    public static void deleteTopic(){
        ZkUtils zkUtils = ZkUtils.apply("localhost:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
        // 删除topic 't1'
        AdminUtils.deleteTopic(zkUtils, "t1");
        zkUtils.close();
    }

    public static void queryTopic(){
        ZkUtils zkUtils = ZkUtils.apply("localhost:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
        // 获取topic 'test'的topic属性属性
        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "t1");
        // 查询topic-level属性
        Iterator it = props.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry entry=(Map.Entry)it.next();
            Object key = entry.getKey();
            Object value = entry.getValue();
            System.out.println(key + " = " + value);
        }
        zkUtils.close();
    }


}
