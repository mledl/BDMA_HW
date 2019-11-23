import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;

import java.util.Properties;

public class FraudTxsPredictor {

    final static Logger logger = Logger.getLogger(FraudTxsPredictor.class);

    public void run() {

        DecisionTreeClassificationModel model = DecisionTreeClassificationModel.load("/Users/pawelurbanowicz/BDMA_HW/Project/modelbuilder/model");

        Properties props = KafkaConfiguration.getStreamProperties();

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(KafkaConfiguration.NEW_TXS_TOPIC)
                .filter((value, key) -> {
                    //TODO  call model , prepare input
                    return true;
                }).to(KafkaConfiguration.FRAUD_TXS_TOPIC);

        new KafkaStreams(builder.build(), props)
                .start();

        logger.error("started");
    }

}
