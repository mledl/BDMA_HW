import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class KafkaConfiguration {
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

}