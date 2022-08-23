package chapter2;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class CompanyProducer {

    public static final String TOPIC_DEMO = "topic-demo";
    public static final String BROKER_LIST = "10.211.55.21:9092";

    public static void main(String[] args) {
        // init producerRecord
        ProducerRecord<String, Company> producerRecord = new ProducerRecord<>(TOPIC_DEMO, new Company("name", "address"));

        // init Properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        // init Serializer
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());

        // init Partitional
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CompanyPartitional.class.getName());

        // init interceptorS
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CompanyFirstInterceptor.class.getName() + "," + CompanySecondInterceptor.class.getName());

        // init kafkaProducer
        try (KafkaProducer<String, Company> kafkaProducer = new KafkaProducer<>(properties)) {
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                log.info(metadata.toString());
            });
        }
    }
}

