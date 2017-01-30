package actors;

import akka.actor.AbstractActor;
import akka.japi.pf.ReceiveBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import configuration.Configuration;
import exceptions.DatasetStatusException;
import models.DatasetStatus;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import play.Logger;
import services.DatasetStatusUpdater;
import utils.KafkaUtils;

import java.util.List;
import java.util.Properties;

import static java.util.Collections.singletonList;

/**
 * Consumes messages arriving over an Apache Kafka topic and checks the completion status of the datasets identified in the messages.
 * The status of the {@link uk.co.onsdigital.discovery.model.DimensionalDataSet} is updated and, if not yet complete, a new message sent to the topic.
 */
public class KafkaDatasetCompletionActor extends AbstractActor {

    private static final Logger.ALogger logger = Logger.of(KafkaDatasetCompletionActor.class);
    private static final long DEAD_DATASET_THRESHOLD = 1000l * 60 * 15; // 15 minutes of inactivity
    private final KafkaClient client = KafkaDatasetCompletionActor.setup();

    public KafkaDatasetCompletionActor(DatasetStatusUpdater completionChecker) {
        receive(ReceiveBuilder.
                match(String.class, s -> {
                    logger.debug("Received String message: {}", s);
                    ConsumerRecords<String, String> records = client.consumer.poll(1000);
                    List<String> jsonRecords = KafkaUtils.recordValues(records);
                    if (jsonRecords.size() >0) {
                        logger.info("Processing {} dataset-status message(s)", jsonRecords.size());
                        try {
                            List<DatasetStatus> incompleteDatasets = completionChecker.checkStatus(jsonRecords);
                            processMessages(incompleteDatasets);
                        } catch (DatasetStatusException | JsonProcessingException ex) {
                            logger.error("Caught error processing dataset status: {}", ex.getMessage());
                            throw new RuntimeException(ex);
                        }
                    }
                }).
                matchAny(o -> logger.info("received unknown message {}", o)).build()
        );
    }

    private void processMessages(List<DatasetStatus> incompleteDatasets) throws JsonProcessingException {
        for (DatasetStatus status : incompleteDatasets) {
            if (System.currentTimeMillis() - status.getLastUpdateTime() > DEAD_DATASET_THRESHOLD) {
                logger.error("Dataset {}: {} of {} rows processed after timeout - sending to dead dataset topic", status.getDatasetID(), status.getRowsProcessed(), status.getTotalRows());
                client.sendDeadDatasetMessage(status);
            } else {
                logger.info("Dataset {}: {} of {} rows processed - returning message to topic", status.getDatasetID(), status.getRowsProcessed(), status.getTotalRows());
                client.sendStatusMessage(status);
            }
        }
    }

    private static KafkaClient setup() {
        String kafkaAddress = Configuration.getKafkaAddress();
        String kafkaConsumerTopic = Configuration.getKafkaDatasetStatusTopic();
        String kafkaConsumerGroup = Configuration.getKafkaConsumerGroup();

        Properties props = new Properties();
        // shared properties
        props.put("bootstrap.servers", kafkaAddress);
        props.put("group.id", kafkaConsumerGroup);
        // consumer properties
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        KafkaClient client = new KafkaClient();
        client.consumer = new KafkaConsumer<>(props);
        client.consumer.subscribe(singletonList(kafkaConsumerTopic));

        // producer properties
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");
        props.put("retries", "5");

        client.producer = new KafkaProducer<>(props);
        client.statusTopic = kafkaConsumerTopic;
        client.deadTopic = Configuration.getKafkaDeadDatasetTopic();
        return client;
    }


    private static final class KafkaClient {
        private final ObjectMapper jsonMapper = new ObjectMapper();
        KafkaConsumer<String, String> consumer;
        KafkaProducer<String, String> producer;
        String statusTopic;
        String deadTopic;

        void sendStatusMessage(DatasetStatus status) throws JsonProcessingException {
            producer.send(new ProducerRecord<String, String>(statusTopic, jsonMapper.writeValueAsString(status)));
        }

        void sendDeadDatasetMessage(DatasetStatus status) throws JsonProcessingException {
            producer.send(new ProducerRecord<String, String>(deadTopic, jsonMapper.writeValueAsString(status)));
        }
    }
}


