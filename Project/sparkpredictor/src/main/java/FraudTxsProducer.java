import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;

public class FraudTxsProducer {

    public void run() {

        final Producer<Long, String> producer = new KafkaProducer<>(KafkaConfiguration.getProducerProperties());
        long time = System.currentTimeMillis();
        try {
            final ProducerRecord<Long, String> record =
                    new ProducerRecord<>(KafkaConfiguration.FRAUD_TXS_TOPIC, 1L,
                            "Hello Mom " + 1);
            RecordMetadata metadata = producer.send(record).get();
            long elapsedTime = System.currentTimeMillis() - time;
            System.out.printf("sent record(key=%s value=%s) " +
                            "meta(partition=%d, offset=%d) time=%d\n",
                    record.key(), record.value(), metadata.partition(),
                    metadata.offset(), elapsedTime);
        } catch (InterruptedException | ExecutionException ee) {
            System.out.println(ee);
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
