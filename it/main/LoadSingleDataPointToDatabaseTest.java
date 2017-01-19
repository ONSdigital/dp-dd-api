package main;

import configuration.Configuration;
import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeGroups;
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

import java.util.Map;
import java.util.UUID;

import static junit.framework.Assert.assertNotNull;

public class LoadSingleDataPointToDatabaseTest extends TestNGSuite {

    private EntityManagerFactory emf;
    private EntityManager em;

    static Logger.ALogger logger = Logger.of(LoadSingleDataPointToDatabaseTest.class);

    static String datasetId = UUID.randomUUID().toString();

    private PostgresTest postgresTest;

    @BeforeClass
    public void setupJPA() {

        logger.info("SETTING UP JPA");
        final Map<String, Object> databaseParameters = Configuration.getDatabaseParameters();
        emf = Persistence.createEntityManagerFactory("data_discovery", databaseParameters);
        em = emf.createEntityManager();
    }

    @BeforeClass
    public void setupDb() {

        logger.info("SETTING UP DB");
        postgresTest = new PostgresTest();
    }

    @Test
    public void addSingleDataPointDirectly() throws Exception {

        logger.info("RUNNING addSingleDataPointDirectly");

        String rowData = "676767,,,,,,K02000001,2014,Year,,NACE,1012 - Processing and preserving of poultry meat,Prodcom Elements,Non production income";

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

            new InputCSVParser().parseRowdataDirectToTables(em, rowDataArray, dimensionalDataSet);

            DimensionalDataPoint result = em.createQuery("SELECT ddp FROM DimensionalDataPoint ddp WHERE ddp.value = 676767", DimensionalDataPoint.class).getSingleResult();

            assertNotNull(result);

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            tx.commit();
        }
    }

}
