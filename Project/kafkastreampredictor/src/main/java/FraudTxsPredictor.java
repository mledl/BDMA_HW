import com.google.gson.JsonParser;
import dto.FraudPredictionDTO;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.log4j.Logger;

import java.util.Properties;

public class FraudTxsPredictor {

    private static final Logger logger = Logger.getLogger(FraudTxsPredictor.class);
    private final double FRAUD_SCORE_THRESHOLD = AppProperties.getPropertyAsDouble("modelservice.fraud_score_threshold");

    public void run() {

        Properties props = KafkaConfiguration.getStreamProperties();

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(KafkaConfiguration.NEW_TXS_TOPIC)
                .filter((key, value) -> {
                    String tx = new JsonParser().parse(value.toString()).getAsString();

                    FraudPredictionDTO fraudPrediction = FraudPredictionModel.makePrediction(tx);

                    return fraudPrediction.getScore() > FRAUD_SCORE_THRESHOLD;
                }).to(KafkaConfiguration.FRAUD_TXS_TOPIC);

        new KafkaStreams(builder.build(), props)
                .start();

        logger.info("started");
    }

}
