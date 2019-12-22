import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaUtils {
    private static final Logger logger = Logger.getLogger(FraudTxsPredictor.class);

    public static final String NEW_TXS_TOPIC = "new_txs";
    public static final String FRAUD_TXS_TOPIC = "fraud_txs";

    public static Properties getStreamProperties() {
        Properties props = getProperties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppProperties.getProperty("kafka.app_id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppProperties.getProperty("kafka.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return props;
    }

    private static Properties getProperties() {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppProperties.getProperty("kafka.servers"));
        return prop;
    }

    public static void waitForKafka() {
        Properties properties = getProperties();

//        properties.put("connections.max.idle.ms", 10000);
//        properties.put("request.timeout.ms", 5000);

        try (AdminClient client = KafkaAdminClient.create(properties)) {
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