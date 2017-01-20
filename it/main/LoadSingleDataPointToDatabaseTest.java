package main;

import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import play.Logger;
import services.InputCSVParser;
import uk.co.onsdigital.discovery.model.DataResource;
import uk.co.onsdigital.discovery.model.DimensionalDataPoint;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;

import java.util.Arrays;
import java.util.UUID;

import static junit.framework.Assert.assertNotNull;
import static main.PostgresTest.AREA_TYPES;
import static main.PostgresTest._2011GPH_SMALL;

public class LoadSingleDataPointToDatabaseTest extends TestNGSuite {

    static Logger.ALogger logger = Logger.of(LoadSingleDataPointToDatabaseTest.class);

    private EntityManagerFactory emf;
    private EntityManager em;
    private PostgresTest postgresTest;





    @BeforeClass
    public void setupDb() throws Exception {
        logger.info("SETTING UP DB");
        postgresTest = new PostgresTest();
        emf = postgresTest.getEMFForEmptyTestDatabase();
        em = emf.createEntityManager();
    }


    @Test
    public void addSingleDataPointDirectly() throws Exception {

        logger.info("RUNNING addSingleDataPointDirectly");

        String rowData = "676767,,,,,,K04000001,2014,Year,,NACE,1012 - Processing and preserving of poultry meat,Prodcom Elements,Non production income";
        String datasetId = UUID.randomUUID().toString();

        String[] rowDataArray = rowData.split(",");
        EntityTransaction tx = em.getTransaction();
        tx.begin();

        try {

            postgresTest.loadStandingData(em, Arrays.asList(AREA_TYPES, _2011GPH_SMALL));

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
            tx.rollback();
        }
    }

}
