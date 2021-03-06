package actors;

import main.PostgresTest;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.Test;
import play.Logger;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import java.util.Properties;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(enabled = false, groups="int-test")
public class KafkaPollingActorTest {

    private EntityManagerFactory emf;
    private EntityManager em;

    static Logger.ALogger logger = Logger.of(KafkaPollingActorTest.class);

    private PostgresTest postgresTest;

    @BeforeGroups("int-test")
    public void setupJPA() {

        logger.info("SETTING UP JPA");
        emf = Persistence.createEntityManagerFactory("data_discovery");
        em = emf.createEntityManager();
    }

    @BeforeGroups("int-test")
    public void setupDb() {

        logger.info("SETTING UP DB");
        postgresTest = new PostgresTest();
    }


    public void kickTheKafkaPolling() throws Exception {
        createDb();


        KafkaActorSingleton.createActorToPollKafka();

        Thread.sleep(3000);

        int messagesToSend = 3;
        sendSomeKafkaMessages(messagesToSend);
        assertEquals(em.createQuery("SELECT ddp FROM DimensionalDataPoint ddp").getResultList().size(), messagesToSend);

    }


    private void sendSomeKafkaMessages(int messagesToSend) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", UUID.randomUUID().toString());
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        String jsonMsgStart = "{\"index\":10619,\"filename\":\"test.csv\",\"startTime\":1481214210,\"datasetID\":\"ac31776f-17a8-4e68-a673-e19589b23496\",\"row\":\"";
        String jsonMsgEnd = ",,,,,,,,,,,,,,,,,%1$d,%1$d,,Year,,,,,,,,,,,,,,,NACE,NACE,,08,08 - Other mining and quarrying,,,,Prodcom Elements,Prodcom Elements,,UK manufacturer sales ID,UK manufacturer sales LABEL,,,\"}";

        for (int i = 0; i < messagesToSend; i++) {
            String messageString = jsonMsgStart + i + String.format(jsonMsgEnd, 2014 + i);
            producer.send(new ProducerRecord<>("test", messageString));
            Thread.sleep(3000);
        }
    }


    private void createDb() {
        try {
            EntityTransaction tx = em.getTransaction();
            tx.begin();

            // TODO - do something

            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }
}
