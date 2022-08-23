package chapter2.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CompanyPartitional implements Partitioner {
    private static final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        System.out.println(this.getClass().getSimpleName() + ": partition");
        Integer countForTopic = cluster.partitionCountForTopic(topic);
        countForTopic = countForTopic == null ? 0 : countForTopic;
        if (null == keyBytes) {
            int i = counter.getAndIncrement() % countForTopic;
            return i;
        } else {
            return Utils.toPositive(Utils.murmur2(keyBytes)) % countForTopic;
        }
    }

    @Override
    public void close() {
        System.out.println(this.getClass().getSimpleName() + ": close");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        System.out.println(this.getClass().getSimpleName() + ": configure");
    }
}
