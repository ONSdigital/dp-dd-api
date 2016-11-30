package actors;

import akka.actor.AbstractActor;
import akka.japi.pf.ReceiveBuilder;
import au.com.bytecode.opencsv.CSVParser;
import models.DataResource;
import models.DimensionalDataSet;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONException;
import org.json.JSONObject;
import play.Logger;
import services.InputCSVParser;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class KafkaActor extends AbstractActor {

    EntityManagerFactory emf = Persistence.createEntityManagerFactory("data_discovery");
    EntityManager em = emf.createEntityManager();
    static Logger.ALogger logger = Logger.of(KafkaActor.class);

    public KafkaActor() {

        receive(ReceiveBuilder.
                match(String.class, s -> {
                    logger.info("Received String message: {}", s);
                    pollKafka();
                }).
                matchAny(o -> logger.info("received unknown message")).build()
        );
    }


    public static KafkaConsumer<String, String> consumer = KafkaActor.setup();

    private static KafkaConsumer<String, String> setup() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", UUID.randomUUID().toString());
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test"));
        return consumer;
    }


    public void pollKafka() throws JSONException, IOException {
        ConsumerRecords<String, String> records = consumer.poll(1000);

        logger.debug("So that'll be a consumer.poll then.");

        for (ConsumerRecord<String, String> record : records) {
            logger.debug("!!!!!Ooooo - a record was found!!!!!");
            System.out.println(record.offset() + ": " + record.value());

            JSONObject rowJson = new JSONObject(record.value());
            String rowData = rowJson.getString("datapoint");

            String[] rowDataArray = new CSVParser().parseLine(rowData);
            logger.debug("rowDataArray: " + rowDataArray.toString());


            try {
                EntityTransaction tx = em.getTransaction();
                tx.begin();


                DataResource dataResource = em.find(DataResource.class, "666");
                if (dataResource == null) {
                    dataResource = new DataResource("666", "title");
                    em.persist(dataResource);
                }

                DimensionalDataSet dimensionalDataSet;
                List<DimensionalDataSet> dimensionalDataSets = dataResource.getDimensionalDataSets();
                if (!dimensionalDataSets.isEmpty() && dimensionalDataSets.get(0) != null) {
                    dimensionalDataSet = dataResource.getDimensionalDataSets().get(0);
                } else {
                    dimensionalDataSet = new DimensionalDataSet("title", dataResource);
                    em.persist(dimensionalDataSet);
                }

                new InputCSVParser().parseRowdataDirectToTables(em, rowDataArray, dimensionalDataSet);

                tx.commit();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}


