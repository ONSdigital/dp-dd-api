package actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import main.PostgresTest;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.annotations.Test;
import scala.concurrent.duration.Duration;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class KafkaPollingActorTest {

    EntityManagerFactory emf = Persistence.createEntityManagerFactory("data_discovery");
    EntityManager em = emf.createEntityManager();

    PostgresTest postgresTest = new PostgresTest();

    @Test(enabled = false)
    public void kickTheKafkaPolling() throws Exception {

        try {
            EntityTransaction tx = em.getTransaction();
            tx.begin();

            postgresTest.createDatabase(em);

            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        ActorSystem system = ActorSystem.create("KafkaActorSystem");
        final ActorRef listener = system.actorOf(Props.create(KafkaActor.class), "listener");

        system.scheduler().schedule(
                Duration.create(0, TimeUnit.MILLISECONDS),
                Duration.create(50, TimeUnit.MILLISECONDS),
                listener,
                "tick",
                system.dispatcher(),
                null
        );


        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", UUID.randomUUID().toString());
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        String jsonMsgStart = "{\"id\":\"\",\"datapoint\":\"";
        String jsonMsgEnd = ",,,,,,,,,,,,,,,,,2014,2014,,Year,,,,,,,,,,,,,,,NACE,NACE,,08,08 - Other mining and quarrying,,,,Prodcom Elements,Prodcom Elements,,UK manufacturer sales ID,UK manufacturer sales LABEL,,,\"}";

        for (int i = 0; i < 3; i++) {
            String messageString = jsonMsgStart + i + jsonMsgEnd;
            producer.send(new ProducerRecord<>("test", messageString));
            Thread.sleep(3000);
        }

        assertEquals(em.createQuery("SELECT ddp FROM DimensionalDataPoint ddp").getResultList().size(), 3);

    }
}
