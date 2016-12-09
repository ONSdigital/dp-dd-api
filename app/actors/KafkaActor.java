package actors;

import akka.actor.AbstractActor;
import akka.japi.pf.ReceiveBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import play.Logger;
import services.DataPointMapper;
import utils.KafkaUtils;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static java.util.Collections.singletonList;

/**
 * Consumes messages arriving from the CSV Splitter and processes them. Messages arrive over an Apache Kafka topic and
 * are turned into {@link models.DimensionalDataPoint} elements to be inserted into the database.
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
                    dataPointMapper.mapDataPoints(jsonRecords);
                }).
                matchAny(o -> logger.info("received unknown message")).build()
        );
    }

    private static KafkaConsumer<String, String> setup() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", UUID.randomUUID().toString());
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(singletonList("test"));
        return consumer;
    }


}


