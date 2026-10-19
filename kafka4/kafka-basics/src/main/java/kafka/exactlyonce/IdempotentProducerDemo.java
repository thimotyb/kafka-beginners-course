package kafka.exactlyonce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * CASO 2 - Producer idempotente: con enable.idempotence=true (default dalla 3.0) il broker assegna al producer
 * un producerId e ogni batch porta un numero di sequenza per partizione: un retry dello stesso batch
 * viene riconosciuto e scartato, quindi niente duplicati dovuti ai retry.
 * Argomento "true" (default) o "false". Per vedere producerId e sequence sul log del broker: kafka-dump-log.sh.
 */
public class IdempotentProducerDemo {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(IdempotentProducerDemo.class.getName());

        String idempotence = args.length > 0 ? args[0] : "true";
        String topic = "eo-idempotent-" + idempotence;

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotence);
        if (idempotence.equals("false")) {
            // senza idempotenza non c'e' obbligo di acks=all
            properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        }

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        logger.info("enable.idempotence=" + idempotence + " -> topic " + topic);

        // 5 messaggi in un unico batch (stessa partizione): il batch porta producerId e numero di sequenza
        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<String, String>(topic, "msg " + i), (metadata, e) -> {
                if (e == null) {
                    logger.info("Scritto: partition " + metadata.partition() + ", offset " + metadata.offset());
                } else {
                    logger.error("Errore", e);
                }
            });
        }
        // flush: aspetta che il batch sia stato scritto e confermato dal broker prima di chiudere
        producer.flush();
        producer.close();
    }
}
