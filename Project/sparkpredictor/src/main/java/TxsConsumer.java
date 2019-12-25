import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.util.Arrays;
import java.util.Properties;

public class TxsConsumer {

    public void run(){

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaConfiguration.getConsumerProperties());

        consumer.subscribe(Arrays.asList(KafkaConfiguration.NEW_TXS_TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);

            records.forEach(record -> {
                System.out.printf("%s [%d] offset=%d, key=%s, value=\"%s\"\n",
                        record.topic(), record.partition(),
                        record.offset(), record.key(), record.value());
            });

            consumer.commitAsync();
        }
    }
}
