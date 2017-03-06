package actors;

import akka.actor.AbstractActor;
import akka.japi.pf.ReceiveBuilder;
import exceptions.DatasetStatusException;
import play.Logger;
import services.DatasetStatusUpdater;

/**
 * Consumes messages arriving over an Apache Kafka topic and checks the completion status of the datasets identified in the messages.
 * The status of the {@link uk.co.onsdigital.discovery.model.DataSet} is updated and, if not yet complete, a new message sent to the topic.
 */
public class KafkaDatasetCompletionActor extends AbstractActor {

    private static final Logger.ALogger logger = Logger.of(KafkaDatasetCompletionActor.class);
    private final KafkaDatasetStatusClient client = KafkaDatasetStatusClient.createClient();

    public KafkaDatasetCompletionActor(DatasetStatusUpdater statusUpdater) {
        receive(ReceiveBuilder.
                match(String.class, s -> {
                    logger.debug("Received String message: {}", s);
                    try {
                        client.processStatusMessages(statusUpdater);
                    } catch (DatasetStatusException e) {
                        logger.error("DatasetStatusException caught processing status messages", e);
                        throw e;
                    }
                }).
                matchAny(o -> logger.info("received unknown message {}", o)).build()
        );
    }
}


