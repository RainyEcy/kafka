package chapter2;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CompanySerializer implements Serializer<Company> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Company data) {
        String jsonString = JSON.toJSONString(data);
        return jsonString.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {

    }
}
