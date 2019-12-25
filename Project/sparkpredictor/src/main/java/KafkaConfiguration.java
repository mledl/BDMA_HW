import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaConfiguration {
    public static final String SERVERS = "localhost:9092";
    public static final String NEW_TXS_TOPIC = "new_txs";
    public static final String FRAUD_TXS_TOPIC = "fraud_txs";
    public static final long SLEEP_TIMER = 1000;

    public static Properties getConsumerProperties(){
        String deserializer = StringDeserializer.class.getName();

        Properties prop = getProperties();
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "sparkpredictor");

        return prop;
    }

    public static Properties getProducerProperties(){
        Properties prop = getProperties();
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, "sparkpredictor");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return prop;
    }

    private static Properties getProperties(){
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfiguration.SERVERS);
        return prop;
    }

}