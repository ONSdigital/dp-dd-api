package main;

import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import play.Logger;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import java.util.UUID;

public class LoadV3SAPETest extends TestNGSuite {

    static Logger.ALogger logger = Logger.of(LoadV3SAPETest.class);

    private EntityManagerFactory emf;
    private EntityManager em;
    private PostgresTest postgresTest = new PostgresTest();

    @BeforeClass
    public void setupDb() {
        emf = postgresTest.getEMFForProductionLikeDatabase();
        em = emf.createEntityManager();
    }

    @Test
    public void loadSAPE() throws Exception {
        postgresTest.loadFileAndCheckDimensionCount(em, UUID.randomUUID(), "SAPEDE_3_GEOGRAPHIES_small.csv", 99);
    }

}
