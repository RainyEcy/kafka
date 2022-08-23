package chapter3.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Postion_Committed {

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
            // assign topicPartitional 0
            kafkaConsumer.assign(Collections.singletonList(new TopicPartition(TOPIC_DEMO, 0)));

            // poll
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord.toString());
                    OffsetAndMetadata committed = kafkaConsumer.committed(new TopicPartition(TOPIC_DEMO, 0));
                    System.out.println("kafkaConsumer.committed : " + committed.offset());
                    long position = kafkaConsumer.position(new TopicPartition(TOPIC_DEMO, 0));
                    System.out.println("kafkaConsumer.position : " + position);

                }
            }

        }
    }
}
