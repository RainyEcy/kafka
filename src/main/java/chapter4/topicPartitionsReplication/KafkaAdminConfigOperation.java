package chapter4.topicPartitionsReplication;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaAdminConfigOperation {


    public static final String BOOTSTRAP_SERVER = "10.211.55.21:9092";
    public static final String TOPIC_ADMIN = "topic-admin";

    public static void main(String[] args) {
        // init props
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);

        // init adminClient
        AdminClient adminClient = AdminClient.create(props);

        try (AdminClient client = adminClient) {
            // add topic
            CreateTopicsResult topics = client.createTopics(Collections.singleton(new NewTopic(TOPIC_ADMIN, 3, (short) 2)));
            topics.all().get();

            // add partitions
            Map<String, NewPartitions> newPartitionsMap = Collections.singletonMap(TOPIC_ADMIN, NewPartitions.increaseTo(5));
            CreatePartitionsResult partitions = client.createPartitions(newPartitionsMap);
            partitions.all().get();

            // alter topic config
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_ADMIN);
            ConfigEntry entry = new ConfigEntry("cleanup.policy", "compact");
            Config config = new Config(Collections.singleton(entry));
            Map<ConfigResource, Config> configs = new HashMap<>();
            configs.put(resource, config);
            AlterConfigsResult result = client.alterConfigs(configs);
            result.all().get();

            // describe topic config
            DescribeConfigsResult describeConfigsResult = client.describeConfigs(Collections.singleton(resource));
            Config describeConfig = describeConfigsResult.all().get().get(resource);
            System.out.println(describeConfig);

            // delete topic
            DeleteTopicsResult deleteTopicsResult = client.deleteTopics(Collections.singletonList(TOPIC_ADMIN));
            deleteTopicsResult.all().get();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
