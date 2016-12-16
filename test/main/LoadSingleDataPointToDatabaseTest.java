package main;

import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.Test;
import play.Logger;
import services.InputCSVParser;
import uk.co.onsdigital.discovery.model.DataResource;
import uk.co.onsdigital.discovery.model.DimensionalDataPoint;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import java.util.UUID;

import static junit.framework.Assert.assertNotNull;


public class LoadSingleDataPointToDatabaseTest extends TestNGSuite {

    EntityManagerFactory emf = Persistence.createEntityManagerFactory("data_discovery");
    EntityManager em = emf.createEntityManager();
    static Logger.ALogger logger = Logger.of(LoadSingleDataPointToDatabaseTest.class);

    static String datasetId = UUID.randomUUID().toString();

    PostgresTest postgresTest = new PostgresTest();


    @Test
    public void addSingleDataPointDirectly() throws Exception {


        String rowData = "676767,,,,,,,,,,,,,,,,,2014,2014,,Year,,,,,,,,,,,,,,,NACE,NACE,,08,08 - Other mining and quarrying,,,,Prodcom Elements,Prodcom Elements,,UK manufacturer sales ID,UK manufacturer sales LABEL,,,";

        String[] rowDataArray = rowData.split(",");
        EntityTransaction tx = em.getTransaction();
        tx.begin();

        try {

            postgresTest.createDatabase(em);

            if (em.find(DataResource.class, datasetId) == null) {
                DataResource dataResource = new DataResource(datasetId, "title");
                em.persist(dataResource);

                DimensionalDataSet dimensionalDataSet = new DimensionalDataSet("title", dataResource);
                dimensionalDataSet.setDimensionalDataSetId(UUID.fromString(datasetId));
                dataResource.addDimensionalDataSet(dimensionalDataSet);

                em.persist(dimensionalDataSet);
            }

            DataResource dataResource = em.find(DataResource.class, datasetId);
            DimensionalDataSet dimensionalDataSet = dataResource.getDimensionalDataSets().get(0);

            new InputCSVParser(em).parseRowdataDirectToTables(rowDataArray, dimensionalDataSet);
            em.flush();

            DimensionalDataPoint result = em.createQuery("SELECT ddp FROM DimensionalDataPoint ddp WHERE ddp.value = 676767", DimensionalDataPoint.class).getSingleResult();

            assertNotNull(result);

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            tx.rollback();
        }


    }


}
