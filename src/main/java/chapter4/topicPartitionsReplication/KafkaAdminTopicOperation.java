package chapter4.topicPartitionsReplication;

import org.apache.kafka.clients.admin.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaAdminTopicOperation {

    public static final String BOOTSTRAP_SERVER = "10.211.55.21:9092";
    public static final String TOPIC_ADMIN = "topic-admin";
    public static final String TOPIC_ADMIN_B = "topic-admin-b";

    public static void main(String[] args) {
        // init props
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);

        // init adminClient
        AdminClient adminClient = AdminClient.create(props);

        try (AdminClient client = adminClient) {
            // create topic A
            CreateTopicsResult topics = client.createTopics(Collections.singleton(new NewTopic(TOPIC_ADMIN, 4, (short) 1)));
            topics.all().get();

            // create topic B
            HashMap<Integer, List<Integer>> replicasAssignments = new HashMap<>();
            replicasAssignments.put(0, Arrays.asList(0));
            replicasAssignments.put(1, Arrays.asList(0));
            CreateTopicsResult topicB = client.createTopics(Collections.singleton(new NewTopic(TOPIC_ADMIN_B, replicasAssignments)));
            topicB.all().get();

            // describe topic A
            DescribeTopicsResult describeTopicsResult = client.describeTopics(Collections.singleton(TOPIC_ADMIN));
            Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
            System.out.println(stringTopicDescriptionMap.get(TOPIC_ADMIN));

            // delete topic A and B
            DeleteTopicsResult deleteTopicsResult = client.deleteTopics(Arrays.asList(TOPIC_ADMIN, TOPIC_ADMIN_B));
            deleteTopicsResult.all().get();

        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

}
