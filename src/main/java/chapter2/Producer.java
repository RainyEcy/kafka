package chapter2;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class Producer {

    public static final String CHAPTER_2 = "hello,chapter2";
    public static final String TOPIC_DEMO = "topic-demo";
    public static final String BROKER_LIST = "10.211.55.21:9092";

    public static void main(String[] args) {
        // init producerRecord
        ProducerRecord<String, Company> producerRecord = new ProducerRecord<>(TOPIC_DEMO, new Company("name", "address"));

        // init Properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);


        // init kafkaProducer
        try (KafkaProducer<String, Company> kafkaProducer = new KafkaProducer<>(properties)) {
            int i = 0;
            while (i++ < 5) {
                kafkaProducer.send(producerRecord, (metadata, exception) -> {
                    log.info(metadata.toString());
                });
            }
        }
    }
}

