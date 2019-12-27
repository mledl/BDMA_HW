import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class TxsStreamingCount {

    public void process(JavaInputDStream<ConsumerRecord<String, String>> txs){

        JavaDStream<Long> count = txs.count();

        count.print();

    }

}
