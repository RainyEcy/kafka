package chapter3.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class CommitConsumer {
    public static final String BOOTSTRAP_SERVER = "10.211.55.21:9092";
    public static final String GROUP_DEMO = "group-demo";
    public static final String TOPIC_DEMO = "topic-demo";

    public static void main(String[] args) {
        // init properties
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_DEMO);
        // close Auto_Commit
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // init kafkaConsumer
        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
            // assign topicPartitional 0
            kafkaConsumer.subscribe(Collections.singletonList(TOPIC_DEMO));

            // poll
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

                // commitSync
                // commitSync(kafkaConsumer, consumerRecords);

                // commitSyncWithTopicPartitionAndOffset
                commitSyncWithTopicPartitionAndOffset(kafkaConsumer, consumerRecords);

                // commitAsyncWithTopicPartitionAndOffset
                // commitAsyncWithTopicPartitionAndOffset(kafkaConsumer, consumerRecords);
            }

        }
    }

    private static void commitAsyncWithTopicPartitionAndOffset(KafkaConsumer<String, String> kafkaConsumer, ConsumerRecords<String, String> consumerRecords) {
        for (TopicPartition partition : consumerRecords.partitions()) {
            List<ConsumerRecord<String, String>> recordList = consumerRecords.records(partition);
            for (ConsumerRecord<String, String> record : recordList) {
                System.out.println(record.toString());
            }
            long offset = kafkaConsumer.position(partition);
            // offset = recordList.get(recordList.size() - 1).offset();
            System.out.println(partition.toString() + " : " + offset);
            kafkaConsumer.commitAsync(Collections.singletonMap(partition, new OffsetAndMetadata(offset)), (offsets, exception) -> {
                if (Objects.isNull(exception)) {
                    System.out.println("commitAsync succ: " + offsets);
                } else {
                    System.out.println("commitAsync succ: " + exception.getLocalizedMessage());
                }
            });

        }
    }

    private static void commitSyncWithTopicPartitionAndOffset(KafkaConsumer<String, String> kafkaConsumer, ConsumerRecords<String, String> consumerRecords) {
        for (TopicPartition partition : consumerRecords.partitions()) {
            List<ConsumerRecord<String, String>> recordList = consumerRecords.records(partition);
            for (ConsumerRecord<String, String> record : recordList) {
                System.out.println(record.toString());
            }
            long offset = kafkaConsumer.position(partition);
            // offset = recordList.get(recordList.size() - 1).offset();
            System.out.println(partition.toString() + " : " + offset);
            kafkaConsumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(offset)));
        }
    }

    private static void commitSync(KafkaConsumer<String, String> kafkaConsumer, ConsumerRecords<String, String> consumerRecords) {
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            System.out.println(consumerRecord.toString());
        }
        kafkaConsumer.commitSync();
    }
}
