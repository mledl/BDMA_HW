import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.avro.package$;

public class App {
    private static String GROUP_ID = "fraud_spark";

    public static void main(String[] args) throws StreamingQueryException {
        KafkaCustomUtils.waitForKafka();

        SparkSession spark = SparkSession
                .builder()
                .appName(AppProperties.getProperty("app.name"))
                .master(AppProperties.getProperty("spark.uri"))
                .getOrCreate();

//        String jsonFormatSchema ="{\"type\":\"record\",\"name\":\"value\",\"namespace\":\"txs.new\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"};
        Dataset<Row> txs = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", AppProperties.getProperty("kafka.servers"))
                .option("subscribe", KafkaCustomUtils.NEW_TXS_TOPIC)
                .load()
                .select(package$.MODULE$.from_avro(
                        new Column("value"),
                        AppProperties.getProperty("schemaregistry.uri")).as("new_tx")
                );

//                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        Dataset<Row> counts = txs.groupBy().count();

        StreamingQuery query = counts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();

    }

}