package kafka.exactlyonce;

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
import java.util.UUID;

/**
 * CASO 3a (lettura) - Legge un topic dall'inizio con l'isolation level scelto.
 * Uso: ReadCommittedConsumerDemo <topic> <read_committed|read_uncommitted>
 */
public class ReadCommittedConsumerDemo {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ReadCommittedConsumerDemo.class.getName());

        String topic = args.length > 0 ? args[0] : "eo-tx";
        String isolation = args.length > 1 ? args[1] : "read_committed";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // gruppo nuovo a ogni esecuzione: rilegge sempre dall'inizio
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "eo-reader-" + UUID.randomUUID());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // read_committed: il consumer salta i messaggi delle transazioni abortite e non legge oltre le transazioni ancora aperte
        // read_uncommitted (default): legge tutto quello che c'e' nel log, anche i messaggi poi abortiti
        properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        logger.info("Lettura di " + topic + " con isolation.level=" + isolation);

        // il consumer non ha una fine naturale: si ferma dopo 5 poll consecutive (circa 5 secondi) senza nuovi messaggi,
        // cosi' la demo termina da sola quando il topic e' stato letto tutto
        int idle = 0;
        int count = 0;
        while (idle < 5) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (records.isEmpty()) {
                idle++;
                continue;
            }
            for (ConsumerRecord<String, String> record : records) {
                count++;
                logger.info("Letto: " + record.value() + " (partition " + record.partition() + ", offset " + record.offset() + ")");
            }
        }
        logger.info("Totale messaggi letti: " + count);
        consumer.close();
    }
}
