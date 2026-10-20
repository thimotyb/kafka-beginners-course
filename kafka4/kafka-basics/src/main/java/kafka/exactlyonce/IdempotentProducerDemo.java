package kafka.exactlyonce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Map;
import java.util.Properties;

/**
 * CASO 2 - Producer idempotente: con enable.idempotence=true (default dalla 3.0) il broker assegna al producer
 * un producerId e ogni batch porta un numero di sequenza per partizione: un retry dello stesso batch
 * viene riconosciuto e scartato, quindi niente duplicati dovuti ai retry.
 * Argomento "true" (default) o "false". Per vedere producerId e sequence sul log del broker: kafka-dump-log.sh.
 * Invia 100 messaggi, uno al secondo: durante l'invio si puo' fermare il broker a mano (docker pause/stop) per
 * provocare i retry; il log segnala quando il producer rileva un fallimento e ritenta.
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
        // timeout corto: dopo 5 secondi senza risposta il producer considera fallita la richiesta e la ritenta
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
        if (idempotence.equals("false")) {
            // senza idempotenza non c'e' obbligo di acks=all
            properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        }

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        logger.info("enable.idempotence=" + idempotence + " -> topic " + topic);

        double retriesVisti = 0;
        for (int i = 0; i < 100; i++) {
            final int n = i;
            producer.send(new ProducerRecord<String, String>(topic, "msg " + n), (metadata, e) -> {
                if (e == null) {
                    logger.info("Scritto: msg " + n + ", partition " + metadata.partition() + ", offset " + metadata.offset());
                } else {
                    logger.error("Errore definitivo su msg " + n, e);
                }
            });

            // i retry avvengono dentro il producer e la callback non li vede: li rileviamo dalla metrica record-retry-total
            double retries = metric(producer, "record-retry-total");
            if (retries > retriesVisti) {
                logger.warn("FALLIMENTO + RETRY: il producer sta ritentando (record ritentati finora: " + (long) retries + ")");
                retriesVisti = retries;
            }

            // pausa tra un messaggio e l'altro: tempo per fermare/riavviare il broker a mano
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        double retries = metric(producer, "record-retry-total");
        if (retries > retriesVisti) {
            logger.warn("FALLIMENTO + RETRY: il producer sta ritentando (record ritentati finora: " + (long) retries + ")");
        }
        // flush: aspetta che il batch sia stato scritto e confermato dal broker prima di chiudere
        producer.flush();
        producer.close();
    }

    private static double metric(KafkaProducer<String, String> producer, String name) {
        for (Map.Entry<MetricName, ? extends Metric> entry : producer.metrics().entrySet()) {
            if (entry.getKey().group().equals("producer-metrics") && entry.getKey().name().equals(name)) {
                return ((Number) entry.getValue().metricValue()).doubleValue();
            }
        }
        return 0;
    }
}
