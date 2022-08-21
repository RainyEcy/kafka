package chapter1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 *
 */
@Slf4j
public class Producer {

    public static final String BROKER_LIST = "10.211.55.21:9092";
    public static final String TOPIC_DEMO = "topic-demo";

    public static void main(String[] args) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_DEMO, "hello,kafka!");

        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {
            kafkaProducer.send(producerRecord, (metadata, exception) -> log.info("Send Success:" + metadata.toString()));
        }


    }
}
