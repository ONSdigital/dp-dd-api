package main;

import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import play.Logger;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import static org.testng.Assert.assertEquals;


public class LoadCsvToDatabaseTest extends TestNGSuite {

    EntityManagerFactory emf = Persistence.createEntityManagerFactory("data_discovery");
    EntityManager em = emf.createEntityManager();
    static Logger.ALogger logger = Logger.of(LoadCsvToDatabaseTest.class);

    static String datasetId = "666";

    PostgresTest postgresTest = new PostgresTest();

    @BeforeClass
    public void initialise() throws Exception {
        postgresTest.createDatabase(em);
    }


    @Test
    public void loadACsvIntoDb() throws Exception {
        postgresTest.createDataset(em, datasetId, "Open-Data-small.csv", "Title");
        assertEquals(em.createQuery("SELECT ddp from DimensionalDataPoint ddp where ddp.dimensionalDataSet.dataResourceBean.dataResource = '666'").getResultList().size(), 276);
    }





}
