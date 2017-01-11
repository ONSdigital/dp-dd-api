package actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import configuration.Configuration;
import play.Logger;
import scala.concurrent.duration.Duration;
import services.DataPointMapper;
import services.InputCSVParser;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.persistence.EntityManager;
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
    }

    public static void createActorToPollKafka() {
        ActorSystem system = ActorSystem.create("KafkaActorSystem");

        final Map<String, Object> databaseParameters = Configuration.getDatabaseParameters();
        final EntityManagerFactory emf = Persistence.createEntityManagerFactory("data_discovery", databaseParameters);
        final EntityManager em = emf.createEntityManager();
        final DataPointMapper dataPointMapper = new DataPointMapper(new InputCSVParser(), em);

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
}
