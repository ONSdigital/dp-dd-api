package actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import configuration.Configuration;
import configuration.DbMigrator;
import play.Logger;
import scala.concurrent.duration.Duration;
import services.DataPointMapper;
import services.DatasetStatusUpdater;
import services.InputCSVParserV3;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.persistence.*;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Singleton
public class KafkaActorSingleton {

    private static final Logger.ALogger log = Logger.of(KafkaActor.class);

    private static final Map<String, String> databaseParameters = Configuration.getDatabaseParameters();
    private static final EntityManagerFactory emf = Persistence.createEntityManagerFactory("data_discovery", databaseParameters);

    @Inject
    public KafkaActorSingleton() {
        upgradeDB();
        log.debug("Initialising Kafka listener...");
        createActorToPollKafka();
        createActorToPollKafkaForDatasetStatus();
    }

    /**
     * Invoke Flyway to update the database (cleaning it first if Configuration.isCleanDatabaseFlagSet returns true).
     */
    private void upgradeDB() {
        log.info("Upgrading db");
        DbMigrator dbMigrator = DbMigrator.getMigrator();
        if (Configuration.isCleanDatabaseFlagSet()) {
            dbMigrator.clean();
        }
        dbMigrator.migrate();
    }

    public static void createActorToPollKafka() {
        ActorSystem system = ActorSystem.create("KafkaActorSystem");

        final DataPointMapper dataPointMapper = new DataPointMapper(InputCSVParserV3::new, emf);

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

        DatasetStatusUpdater updater = new DatasetStatusUpdater(emf);

        final ActorRef listener = system.actorOf(Props.create(KafkaDatasetCompletionActor.class, updater), "listener");

        system.scheduler().schedule(
                Duration.Zero(),
                Duration.create(15, TimeUnit.SECONDS),
                listener,
                "tick",
                system.dispatcher(),
                null
        );
    }
}
