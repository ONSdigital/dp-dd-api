package main;

import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import play.Logger;
import uk.co.onsdigital.discovery.model.Hierarchy;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import java.util.Arrays;

import static main.PostgresTest.*;
import static org.testng.AssertJUnit.assertNotNull;

public class LoadHierarchiesTest extends TestNGSuite {

    static Logger.ALogger logger = Logger.of(LoadHierarchiesTest.class);

    private EntityManagerFactory emf;
    private EntityManager em;
    private PostgresTest postgresTest = new PostgresTest();

    @BeforeClass
    public void setupDb() {
        emf = postgresTest.getEMFForEmptyTestDatabase();
        em = emf.createEntityManager();
    }

    @Test
    public void loadMultipleHierarchiesIntoSameDatabase() throws Exception {

        EntityTransaction tx = em.getTransaction();
        tx.begin();
        try {
            postgresTest.loadStandingData(em, Arrays.asList(COICOP));
            Hierarchy result = em.createQuery("SELECT h FROM Hierarchy h WHERE h.id = 'CL_0000641'", Hierarchy.class).getSingleResult();
            assertNotNull(result);

            postgresTest.loadStandingData(em, Arrays.asList(NACE));
            result = em.createQuery("SELECT h FROM Hierarchy h WHERE h.id = 'CL_0001480'", Hierarchy.class).getSingleResult();
            assertNotNull(result);

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            tx.rollback();
        }
    }

}
