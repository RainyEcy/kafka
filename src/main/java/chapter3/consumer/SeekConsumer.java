package chapter3.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class SeekConsumer {

    public static final String GROUP_DEMO = "group-demo";
    public static final String BOOTSTRAP_SERVER = "10.211.55.21:9092";
    public static final String TOPIC_DEMO = "topic-demo";

    public static void main(String[] args) {
        // init properties
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_DEMO);

        // init kafkaConsumer
        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {

            kafkaConsumer.subscribe(Collections.singletonList(TOPIC_DEMO));

            // seek to offset with topic
            Set<TopicPartition> assignment = new HashSet<>();
            while (assignment.size() == 0) {
                kafkaConsumer.poll(Duration.ofMillis(1000));
                assignment = kafkaConsumer.assignment();
            }
            for (TopicPartition topicPartition : assignment) {
                // 自定义Seek位置
                // kafkaConsumer.seek(topicPartition, 150);
                // Seek初始位置
                // kafkaConsumer.seekToBeginning(Collections.singletonList(topicPartition));
                // Seek末尾
                // kafkaConsumer.seekToEnd(Arrays.asList(topicPartition));
                // Seek时间点
                Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = kafkaConsumer.offsetsForTimes(Collections.singletonMap(topicPartition, System.currentTimeMillis() - 1000 * 3600));
                OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetAndTimestampMap.get(topicPartition);
                kafkaConsumer.seek(topicPartition, offsetAndTimestamp.offset());

            }

            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord.toString());
                }
            }
        }

    }
}
