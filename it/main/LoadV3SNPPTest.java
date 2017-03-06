package main;

import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import play.Logger;

import javax.persistence.*;
import java.util.UUID;

public class LoadV3SNPPTest extends TestNGSuite {

    static Logger.ALogger logger = Logger.of(LoadV3SAPETest.class);

    private EntityManager em;
    private PostgresTest postgresTest = new PostgresTest();

    @BeforeClass
    public void setupDb() {
        EntityManagerFactory emf = postgresTest.getEMFForProductionLikeDatabase();
        em = emf.createEntityManager();
    }

    @Test
    public void loadSAPE() throws Exception {
        postgresTest.loadFileAndCheckDimensionCount(em, UUID.randomUUID(), "SNPP_2012_WARDH_2_EN_small.csv", 34);
    }

}
