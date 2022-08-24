package chapter3.consumer;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class InterceptorConsumer implements ConsumerInterceptor<String, String> {
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        System.out.println(this.getClass().getSimpleName() + " : onConsume");
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        System.out.println(this.getClass().getSimpleName() + " : onCommit");
    }

    @Override
    public void close() {
        System.out.println(this.getClass().getSimpleName() + " : close");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        System.out.println(this.getClass().getSimpleName() + " : configure");
    }
}
