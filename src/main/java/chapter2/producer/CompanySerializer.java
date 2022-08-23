package chapter2.producer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CompanySerializer implements Serializer<Company> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        System.out.println(this.getClass().getSimpleName() + ": configure");

    }

    @Override
    public byte[] serialize(String topic, Company data) {
        System.out.println(this.getClass().getSimpleName() + ": serialize");
        String jsonString = JSON.toJSONString(data);
        return jsonString.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        System.out.println(this.getClass().getSimpleName() + ": close");

    }
}
