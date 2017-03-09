package main;

import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import play.Logger;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import java.util.UUID;

public class LoadV3InternalMigrationTest extends TestNGSuite {

    static Logger.ALogger logger = Logger.of(LoadV3InternalMigrationTest.class);

    private EntityManager em;
    private PostgresTest postgresTest = new PostgresTest();

    @BeforeClass
    public void setupDb() {
        EntityManagerFactory emf = postgresTest.getEMFForProductionLikeDatabase();
        em = emf.createEntityManager();
    }

    @Test
    public void loadInternalMigration() throws Exception {
        postgresTest.loadFileAndCheckDimensionCount(em, UUID.randomUUID(), "Internal_Migration_small.csv", 118);
    }

}
