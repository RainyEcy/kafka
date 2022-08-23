package chapter1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consumer 消费者
 */
@Slf4j
public class FastConsumer {

    public static final String BOOTSTRAP_SERVERS = "10.211.55.21:9092";
    public static final String GROUP_DEMO = "group-demo";
    public static final String TOPIC_DEMO = "topic-demo";
    public static final AtomicBoolean ATOMIC_BOOLEAN = new AtomicBoolean(true);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_DEMO);

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
            kafkaConsumer.subscribe(Collections.singletonList(TOPIC_DEMO));

            while (ATOMIC_BOOLEAN.get()) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info(consumerRecord.toString());
                }
            }


        }

    }
}
