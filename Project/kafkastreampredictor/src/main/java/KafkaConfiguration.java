import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class KafkaConfiguration {
    public static final String SERVERS = "localhost:9092";
    public static final String NEW_TXS_TOPIC = "new_txs";
    public static final String FRAUD_TXS_TOPIC = "fraud_txs";
    public static final long SLEEP_TIMER = 1000;

    public static Properties getStreamProperties(){
        Properties props = getProperties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafkastream-predictor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return props;
    }

    private static Properties getProperties(){
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfiguration.SERVERS);
        return prop;
    }

}