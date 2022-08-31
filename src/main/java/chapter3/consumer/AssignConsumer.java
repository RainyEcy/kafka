package chapter3.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AssignConsumer {

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

        // init kafkaConsumer
        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
            // assign TopicPartition
            List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(TOPIC_DEMO);
            ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
            partitionInfos.forEach(partitionInfo -> topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition())));
            kafkaConsumer.assign(topicPartitions);

            // poll
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord.toString());
                }
            }

        }
    }
}
