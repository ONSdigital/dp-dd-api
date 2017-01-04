package main;

import models.DataResource;
import models.DimensionalDataPoint;
import models.DimensionalDataSet;
import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.Test;
import play.Logger;
import services.InputCSVParser;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import static junit.framework.Assert.assertNotNull;


public class LoadSingleDataPointToDatabaseTest extends TestNGSuite {

    EntityManagerFactory emf = Persistence.createEntityManagerFactory("data_discovery");
    EntityManager em = emf.createEntityManager();
    static Logger.ALogger logger = Logger.of(LoadSingleDataPointToDatabaseTest.class);

    static String datasetId = "666";

    private PostgresTest postgresTest;

    @BeforeGroups("int-test")
    public void setupDb() {

        logger.info("SETTING UP DB");
        postgresTest = new PostgresTest();
    }

    @Test(groups="int-test")
    public void addSingleDataPointDirectly() throws Exception {

        logger.info("RUNNING addSingleDataPointDirectly");

        String rowData = "676767,,,,,,,,,,,,,,,,,2014,2014,,Year,,,,,,,,,,,,,,,NACE,NACE,,08,08 - Other mining and quarrying,,,,Prodcom Elements,Prodcom Elements,,UK manufacturer sales ID,UK manufacturer sales LABEL,,,";

        String[] rowDataArray = rowData.split(",");
        try {
            EntityTransaction tx = em.getTransaction();
            tx.begin();

            postgresTest.createDatabase(em);

            if (em.find(DataResource.class, "666") == null) {
                DataResource dataResource = new DataResource(datasetId, "title");
                em.persist(dataResource);

                DimensionalDataSet dimensionalDataSet = new DimensionalDataSet("title", dataResource);
                dataResource.addDimensionalDataSet(dimensionalDataSet);

                em.persist(dimensionalDataSet);
            }

            DataResource dataResource = em.find(DataResource.class, "666");
            DimensionalDataSet dimensionalDataSet = dataResource.getDimensionalDataSets().get(0);

            new InputCSVParser().parseRowdataDirectToTables(em, rowDataArray, dimensionalDataSet);


            DimensionalDataPoint result = em.createQuery("SELECT ddp FROM DimensionalDataPoint ddp WHERE ddp.value = 676767", DimensionalDataPoint.class).getSingleResult();

            assertNotNull(result);

            tx.rollback();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }


    }


}
