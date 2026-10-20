package kafka.exactlyonce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * CASO 3b - Consume-transform-produce exactly-once: legge da eo-input, scrive in maiuscolo su eo-output
 * e salva gli offset del consumer NELLA STESSA transazione dell'output.
 * Argomento "abort":  scrive l'output ma annulla la transazione (simula un guasto) -> output e offset si annullano insieme.
 * Argomento "commit": committa -> output e offset vengono resi visibili insieme.
 */
public class ExactlyOnceProcessorDemo {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ExactlyOnceProcessorDemo.class.getName());

        boolean abort = args.length > 0 && args[0].equals("abort");

        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "eo-processor");
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // gli offset li committa la transazione, non il consumer
        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // non leggere messaggi di transazioni non committate (utile se l'input e' scritto da un producer transazionale)
        consumerProps.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "eo-processor-tx");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProps);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);
        // registra il transactional.id e chiude eventuali transazioni rimaste aperte da un'istanza precedente (fencing)
        producer.initTransactions();
        consumer.subscribe(Arrays.asList("eo-input"));

        // il consumer non ha una fine naturale: si ferma dopo 5 poll consecutive (circa 5 secondi) senza nuovi messaggi,
        // cosi' la demo termina da sola quando il topic e' stato letto tutto
        int idle = 0;
        while (idle < 5) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (records.isEmpty()) {
                idle++;
                continue;
            }

            // 1) apre la transazione: da qui output e offset saranno atomici
            producer.beginTransaction();
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
            for (ConsumerRecord<String, String> record : records) {
                // 2) "transform" + "produce": scrive il risultato sul topic di output (ancora invisibile ai read_committed)
                producer.send(new ProducerRecord<String, String>("eo-output", record.key(), record.value().toUpperCase()));
                // prepara il "segnalibro" del consumer (quello che farebbe consumer.commitSync()): per ogni partizione
                // la posizione da cui riprendere a leggere; la mappa viene sovrascritta a ogni record, quindi alla fine
                // contiene l'ultimo record letto per partizione (un poll puo' contenere record di piu' partizioni).
                // + 1 perche' l'offset committato e' quello del PROSSIMO messaggio da leggere: senza, alla ripartenza
                // si rileggerebbe l'ultimo messaggio gia' elaborato.
                offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
            }

            if (abort) {
                // flush: cosi' i record arrivano davvero al broker prima dell'abort (e restano non visibili ai read_committed)
                producer.flush();
                producer.abortTransaction();
                logger.info("ABORT: " + records.count() + " messaggi scritti in output ma annullati, offset NON committati");
                break;
            }
            // 3) aggiunge alla stessa transazione gli offset del consumer (al posto di consumer.commitSync()):
            //    il segnalibro e i messaggi su eo-output diventano visibili insieme, oppure spariscono insieme (abort/guasto).
            //    Cosi' non esistono ne' "output scritto ma offset non avanzato" (duplicati) ne' "offset avanzato ma output perso"
            producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
            // 4) commit: output e offset diventano visibili insieme, oppure (abort/guasto) nessuno dei due
            producer.commitTransaction();
            logger.info("COMMIT: " + records.count() + " messaggi in output + offset committati insieme");
        }
        producer.close();
        consumer.close();
    }
}
