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
        // la novita' di Kafka 4: sceglie QUALE protocollo usa il consumer per partecipare al gruppo.
        //  - "classic" (default): JoinGroup/SyncGroup. Un consumer del gruppo, il "group leader", calcola
        //    l'assegnazione delle partizioni LATO CLIENT. A ogni ingresso/uscita di un membro scatta un rebalance con una
        //    barriera globale: tutti i consumer si fermano e restituiscono le partizioni (eager, "stop the world"),
        //    anche quelle che non cambierebbero proprietario, e riprendono solo dopo la nuova assegnazione.
        //  - "consumer" (KIP-848): i membri parlano solo col group coordinator sul broker con heartbeat
        //    (ConsumerGroupHeartbeat) e l'assegnazione la calcola il BROKER, in modo incrementale. Ogni consumer
        //    riceve solo le variazioni che lo riguardano: cede le partizioni che passano ad altri e prende le nuove,
        //    senza fermare gli altri e senza barriera di gruppo. Risultato: rebalance piu' rapidi e meno pause di
        //    consumo nei gruppi grandi o con ingressi/uscite frequenti.
        // Il flag e' solo lato client, ma serve un broker Kafka 4 (nuovo group coordinator). Con "consumer"
        // partition.assignment.strategy e' ignorata: l'assegnatore e' una scelta del broker/gruppo (uniform o range).
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
