package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Kafka 4: nuovo protocollo di rebalance dei consumer group (KIP-848).
 * Il coordinatore sul broker calcola l'assegnazione: niente piu' "stop the world".
 * Si attiva lato client con group.protocol=consumer (default: classic).
 */
public class ConsumerDemoNewProtocol {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoNewProtocol.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-kip848-application";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // la novita' di Kafka 4
        properties.setProperty(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");

        // il resto del codice e' identico a ConsumerDemo: cambia solo chi decide l'assegnazione delle partizioni (il broker)
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
            }
        }
    }
}
