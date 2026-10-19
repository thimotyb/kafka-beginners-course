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

/**
 * CASO 1 - At-least-once: l'elaborazione e il commit dell'offset sono due passi separati.
 * Argomento "crash":  elabora i messaggi e si ferma PRIMA del commit -> al riavvio li rilegge (duplicati).
 * Argomento "commit": elabora e poi fa commitSync() -> il riavvio riparte dopo l'ultimo messaggio.
 */
public class AtLeastOnceConsumerDemo {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(AtLeastOnceConsumerDemo.class.getName());

        boolean crash = args.length > 0 && args[0].equals("crash");

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "eo-at-least-once");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // commit manuale: siamo noi a decidere quando l'offset viene salvato
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("eo-input"));

        // il consumer non ha una fine naturale: si ferma dopo 5 poll consecutive (circa 5 secondi) senza nuovi messaggi,
        // cosi' la demo termina da sola quando il topic e' stato letto tutto
        int idle = 0;
        while (idle < 5) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (records.isEmpty()) {
                // nessun messaggio in questo giro: ci avviciniamo alla fine
                idle++;
                continue;
            }
            for (ConsumerRecord<String, String> record : records) {
                // l'"elaborazione": qui potrebbe esserci una scrittura su DB, una chiamata REST...
                logger.info("ELABORATO: " + record.value() + " (partition " + record.partition() + ", offset " + record.offset() + ")");
            }
            // punto critico: i messaggi sono gia' stati elaborati, ma l'offset non e' ancora stato salvato.
            // se il processo muore qui, al riavvio il gruppo riparte dall'ultimo offset committato -> duplicati
            if (crash) {
                logger.info("CRASH SIMULATO prima del commit: l'offset NON e' stato salvato");
                consumer.close();
                return;
            }
            // salva l'offset DOPO l'elaborazione: se si cade prima di questa riga si rielabora (at-least-once),
            // se si facesse PRIMA dell'elaborazione e si cadesse in mezzo si perderebbero messaggi (at-most-once)
            consumer.commitSync();
            logger.info("Offset committato dopo l'elaborazione");
        }
        consumer.close();
    }
}
