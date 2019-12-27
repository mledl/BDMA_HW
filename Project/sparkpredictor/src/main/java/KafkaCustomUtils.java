import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutionException;

// TODO create separate module
public class KafkaCustomUtils {
    private static final Logger logger = Logger.getLogger(KafkaCustomUtils.class);

    public static final String NEW_TXS_TOPIC = "new_txs";
    public static final String FRAUD_TXS_TOPIC = "fraud_txs";

    private static  Map<String, Object> getParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("bootstrap.servers", AppProperties.getProperty("kafka.servers"));

        return params;
    }

    public static void waitForKafka() {
        Map<String, Object> params = getParams();

        try (AdminClient client = KafkaAdminClient.create(params)) {
            ListTopicsResult topics = client.listTopics();
            Set<String> names = topics.names().get();
            if (names.isEmpty()) {
                logger.warn("Kafka is not available");
                waitForKafka();
            }
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Kafka is not available", e);
            waitForKafka();
        }
    }

}