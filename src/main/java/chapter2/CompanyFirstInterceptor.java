package chapter2;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class CompanyFirstInterceptor implements ProducerInterceptor<String, Company> {
    private static final AtomicLong successCounter = new AtomicLong(0l);

    private static final AtomicLong errorCounter = new AtomicLong(0l);

    @Override
    public ProducerRecord<String, Company> onSend(ProducerRecord<String, Company> record) {
        System.out.println(this.getClass().getSimpleName() + ": onSend");
        String modifiedValue = "Company Interceptor ONE " + record.value().getAddress();
        Company modifiedCompany = new Company(record.value().getName(), modifiedValue);
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), modifiedCompany, record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        System.out.println(this.getClass().getSimpleName() + ": onAcknowledgement");
        if (Objects.isNull(metadata)) {
            errorCounter.incrementAndGet();
        } else {
            successCounter.incrementAndGet();
        }
    }

    @Override
    public void close() {
        System.out.println(this.getClass().getSimpleName() + ": close");

    }

    @Override
    public void configure(Map<String, ?> configs) {
        System.out.println(this.getClass().getSimpleName() + ": configure");
    }
}
