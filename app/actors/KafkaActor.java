package actors;

import akka.actor.AbstractActor;
import akka.japi.pf.ReceiveBuilder;
import configuration.Configuration;
import exceptions.DatapointMappingException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import play.Logger;
import services.DataPointMapper;
import utils.KafkaUtils;

import java.util.List;
import java.util.Properties;

import static java.util.Collections.singletonList;

/**
 * Consumes messages arriving from the CSV Splitter and processes them. Messages arrive over an Apache Kafka topic and
 * are turned into {@link uk.co.onsdigital.discovery.model.DimensionalDataPoint} elements to be inserted into the database.
 */
public class KafkaActor extends AbstractActor {

    private static final Logger.ALogger logger = Logger.of(KafkaActor.class);
    private final KafkaConsumer<String, String> consumer = KafkaActor.setup();

    public KafkaActor(DataPointMapper dataPointMapper) {
        receive(ReceiveBuilder.
                match(String.class, s -> {
                    logger.info("Received String message: {}", s);
                    ConsumerRecords<String, String> records = consumer.poll(1000);
                    List<String> jsonRecords = KafkaUtils.recordValues(records);
                    try {
                        dataPointMapper.mapDataPoints(jsonRecords);
                    } catch (DatapointMappingException ex) {
                        logger.error("Caught error - discarding input: {}", ex.getMessage());
                    }
                }).
                matchAny(o -> logger.info("received unknown message")).build()
        );
    }

    private static KafkaConsumer<String, String> setup() {
        String kafkaAddress = Configuration.getKafkaAddress();
        String kafkaConsumerTopic = Configuration.getKafkaConsumerTopic();
        String kafkaConsumerGroup = Configuration.getKafkaConsumerGroup();

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaAddress);
        props.put("group.id", kafkaConsumerGroup);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(singletonList(kafkaConsumerTopic));
        return consumer;
    }


}


