import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class App {

    public static void main(String[] args) {
        setup();
        FraudTxsPredictor fraudTxsPredictor = new FraudTxsPredictor();
        fraudTxsPredictor.run();

    }

    private static void setup(){
        SparkConf sparkConf =
                new SparkConf()
                        .setAppName("kafkastreampredictor")
                        .setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    }
}