import com.google.gson.Gson;
import dto.FraudPredictionDTO;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class FraudPredictionModel {

    private static final Logger logger = Logger.getLogger(FraudTxsPredictor.class);
    private static final String MODELSERVICE_URI = AppProperties.getProperty("modelservice.uri");

    private static final HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .build();

    public static FraudPredictionDTO makePrediction(String tx) {

        HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(tx))
                .uri(URI.create(MODELSERVICE_URI))
                .header("Content-Type", "application/json;")
                .build();

        HttpResponse<String> response = null;

        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        FraudPredictionDTO fraudPrediction = new Gson().fromJson(response.body(), FraudPredictionDTO.class);

        logger.info("Response: " + fraudPrediction);

        return fraudPrediction;
    }

}