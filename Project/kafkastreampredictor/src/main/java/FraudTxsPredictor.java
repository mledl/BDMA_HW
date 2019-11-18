import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;

public class FraudTxsPredictor {

    final static Logger logger = Logger.getLogger(FraudTxsPredictor.class);

    public void run() {

        Properties props = KafkaConfiguration.getStreamProperties();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> txs = builder.stream(KafkaConfiguration.NEW_TXS_TOPIC);
        KStream<String, String> fraud_txs = txs.filter((s, s2) -> {
//TODO  call model
            return true;
        });

        fraud_txs.to(KafkaConfiguration.FRAUD_TXS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

}
