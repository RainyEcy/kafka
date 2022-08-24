package chapter1.fastStart;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Producer 生产者
 */
@Slf4j
public class FastProducer {

    public static final String BROKER_LIST = "10.211.55.21:9092";
    public static final String TOPIC_DEMO = "topic-demo";

    public static void main(String[] args) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_DEMO, "hello,kafka!");

        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        // 重试次数与间隔时间
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < 4; i++) {
                kafkaProducer.send(producerRecord, (metadata, exception) -> log.info("Send Success:" + metadata.toString()));
            }
        }


    }
}
