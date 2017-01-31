package actors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import configuration.Configuration;
import exceptions.DatasetStatusException;
import models.DatasetStatus;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import play.Logger;
import services.DatasetStatusUpdater;
import utils.KafkaUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.util.Collections.singletonList;

/**
 * Kafka client capable of sending and receiving messages related to dataset status.
 */
public class KafkaDatasetStatusClient {

    private static final long DEAD_DATASET_THRESHOLD = 1000l * 60 * 15; // 15 minutes of inactivity

    private static final Logger.ALogger logger = Logger.of(KafkaDatasetStatusClient.class);

    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final Consumer<String, String> consumer;
    private final Producer<String, String> producer;
    private final String statusTopic;
    private final String deadTopic;


    /**
     * Creates a kafka client.
     * @return A fully configured kafka client capable of polling for messages and creating new messages in response.
     */
    public static KafkaDatasetStatusClient createClient() {
        String kafkaAddress = Configuration.getKafkaAddress();
        String kafkaConsumerTopic = Configuration.getKafkaDatasetStatusTopic();
        String kafkaDeadTopic = Configuration.getKafkaDeadDatasetTopic();
        String kafkaConsumerGroup = Configuration.getKafkaConsumerGroup();

        Properties props = new Properties();
        // shared properties
        props.put("bootstrap.servers", kafkaAddress);
        props.put("group.id", kafkaConsumerGroup);
        // consumer properties
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // producer properties
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");
        props.put("retries", "5");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        return new KafkaDatasetStatusClient(consumer, producer, kafkaConsumerTopic, kafkaDeadTopic);
    }

    KafkaDatasetStatusClient(Consumer<String, String> consumer, Producer<String, String> producer, String statusTopic, String deadTopic) {
        this.consumer = consumer;
        this.consumer.subscribe(singletonList(statusTopic));

        this.producer = producer;
        this.statusTopic = statusTopic;
        this.deadTopic = deadTopic;
    }


    public void processStatusMessages(DatasetStatusUpdater statusUpdater) throws DatasetStatusException {
        List<DatasetStatus> statuses = processStatusMessages();
        if (statuses.size() > 0) {
            try {
                List<DatasetStatus> updated = statusUpdater.updateStatuses(statuses);
                processMessages(updated);
            } catch (JsonProcessingException ex) {
                logger.error("Caught error processing dataset status: {}", ex.getMessage());
                throw new DatasetStatusException(ex);
            }
        }
    }

    private void processMessages(List<DatasetStatus> incompleteDatasets) throws JsonProcessingException {
        for (DatasetStatus status : incompleteDatasets) {
            if (status.isComplete()) {
                logger.info("Dataset {}: {} of {} rows processed - dataset is complete", status.getDatasetID(), status.getRowsProcessed(), status.getTotalRows());
            } else if (System.currentTimeMillis() - status.getLastUpdateTime() > DEAD_DATASET_THRESHOLD) {
                logger.error("Dataset {}: {} of {} rows processed after timeout - sending to dead dataset topic)", status.getDatasetID(), status.getRowsProcessed(), status.getTotalRows());
                sendDeadDatasetMessage(status);
            } else {
                logger.info("Dataset {}: {} of {} rows processed - re-sending message to topic", status.getDatasetID(), status.getRowsProcessed(), status.getTotalRows());
                sendStatusMessage(status);
            }
        }
    }

    private List<DatasetStatus> processStatusMessages() {
        ConsumerRecords<String, String> records = consumer.poll(1000);
        List<String> jsonRecords = KafkaUtils.recordValues(records);
        List<DatasetStatus> statuses = new ArrayList<>(jsonRecords.size());
        for (String jsonRecord : jsonRecords) {
            try {
                logger.info("Received dataset-status message: {}", jsonRecord);
                statuses.add(jsonMapper.readValue(jsonRecord, DatasetStatus.class));
            } catch (IOException e) {
                logger.error("Unable to parse DatasetStatus from {} - ignoring message", jsonRecord, e);
            }
        }
        return statuses;
    }

    private void sendStatusMessage(DatasetStatus status) throws JsonProcessingException {
        producer.send(new ProducerRecord<String, String>(statusTopic, jsonMapper.writeValueAsString(status)));
    }

    private void sendDeadDatasetMessage(DatasetStatus status) throws JsonProcessingException {
        producer.send(new ProducerRecord<String, String>(deadTopic, jsonMapper.writeValueAsString(status)));
    }

}
