package actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import configuration.Configuration;
import play.Logger;
import scala.concurrent.duration.Duration;
import services.DataPointMapper;
import services.DatasetStatusUpdater;
import services.InputCSVParser;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Singleton
public class KafkaActorSingleton {

    private static final Logger.ALogger log = Logger.of(KafkaActor.class);

    @Inject
    public KafkaActorSingleton() {
        log.debug("Initialising Kafka listener...");
        createActorToPollKafka();
        createActorToPollKafkaForDatasetStatus();
    }

    public static void createActorToPollKafka() {
        ActorSystem system = ActorSystem.create("KafkaActorSystem");

        final Map<String, Object> databaseParameters = Configuration.getDatabaseParameters();
        final EntityManagerFactory emf = Persistence.createEntityManagerFactory("data_discovery", databaseParameters);
        final DataPointMapper dataPointMapper = new DataPointMapper(new InputCSVParser(), emf);

        final ActorRef listener = system.actorOf(Props.create(KafkaActor.class, dataPointMapper), "listener");

        system.scheduler().schedule(
                Duration.create(0, TimeUnit.MILLISECONDS),
                Duration.create(50, TimeUnit.MILLISECONDS),
                listener,
                "tick",
                system.dispatcher(),
                null
        );
    }

    public static void createActorToPollKafkaForDatasetStatus() {
        ActorSystem system = ActorSystem.create("KafkaDatasetActorSystem");

        final Map<String, Object> databaseParameters = Configuration.getDatabaseParameters();
        final EntityManagerFactory emf = Persistence.createEntityManagerFactory("data_discovery", databaseParameters);
        DatasetStatusUpdater checker = new DatasetStatusUpdater(emf);

        final ActorRef listener = system.actorOf(Props.create(KafkaDatasetCompletionActor.class, checker), "listener");

        system.scheduler().schedule(
                Duration.create(0, TimeUnit.MILLISECONDS),
                Duration.create(5000, TimeUnit.MILLISECONDS),
                listener,
                "tick",
                system.dispatcher(),
                null
        );
    }
}
