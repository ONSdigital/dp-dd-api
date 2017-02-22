package main;

import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import play.Logger;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import java.util.UUID;

public class LoadV3ProdcomTest extends TestNGSuite {

    static Logger.ALogger logger = Logger.of(LoadV3ProdcomTest.class);

    private EntityManagerFactory emf;
    private EntityManager em;
    private PostgresTest postgresTest = new PostgresTest();

    UUID datasetId;
    DimensionalDataSet dimensionalDataSet;

    @BeforeClass
    public void setupDb() {
        emf = postgresTest.getEMFForProductionLikeDatabase();
        em = emf.createEntityManager();
    }

    @BeforeMethod
    public void setup() {
        datasetId = UUID.randomUUID();
        dimensionalDataSet = postgresTest.createEmptyDataset(em, datasetId.toString(), "dataset");
    }

    @Test
    public void loadProdcom_v3() throws Exception {
        postgresTest.loadFileAndCheckDimensionCount(em, datasetId, "Prodcom_v3.csv", 3747);
    }

}
