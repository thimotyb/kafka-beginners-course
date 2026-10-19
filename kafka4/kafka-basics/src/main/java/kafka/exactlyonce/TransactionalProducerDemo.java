package kafka.exactlyonce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * CASO 3a - Transazioni: scrive 3 messaggi in una transazione COMMITTATA e 3 in una transazione ABORTITA
 * sullo stesso topic (eo-tx). Chi legge con isolation.level=read_committed vede solo i primi 3,
 * chi legge con read_uncommitted li vede tutti e 6 (vedi ReadCommittedConsumerDemo).
 */
public class TransactionalProducerDemo {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(TransactionalProducerDemo.class.getName());

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // il transactional.id identifica il producer anche dopo un riavvio (fencing delle istanze vecchie)
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "eo-tx-producer-1");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        // registra il transactional.id sul broker e chiude eventuali transazioni rimaste aperte da istanze precedenti
        producer.initTransactions();

        // transazione 1: COMMIT
        // i messaggi inviati dopo beginTransaction() restano invisibili ai read_committed fino al commit
        producer.beginTransaction();
        for (int i = 0; i < 3; i++) {
            producer.send(new ProducerRecord<String, String>("eo-tx", "committed " + i));
        }
        // il broker scrive un marker di COMMIT nel log: da qui i 3 messaggi diventano visibili ai read_committed
        producer.commitTransaction();
        logger.info("Transazione 1 COMMITTATA (3 messaggi)");

        // transazione 2: ABORT
        producer.beginTransaction();
        for (int i = 0; i < 3; i++) {
            producer.send(new ProducerRecord<String, String>("eo-tx", "aborted " + i));
        }
        // flush: senza, i record ancora nel buffer vengono scartati dall'abort e non arrivano mai al broker
        producer.flush();
        producer.abortTransaction();
        logger.info("Transazione 2 ABORTITA (3 messaggi)");

        producer.close();
    }
}
