package main;

import models.DataResource;
import models.DimensionalDataSet;
import models.Editor;
import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import play.Logger;
import services.InputCSVParser;
import services.LoadToTarget;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import java.io.File;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class LoadCsvToDatabaseTest extends TestNGSuite {

    static EntityManagerFactory emf = Persistence.createEntityManagerFactory("OnslocalBOPU");
    static EntityManager em = emf.createEntityManager();
    static Logger.ALogger logger = Logger.of(LoadCsvToDatabaseTest.class);

    static String datasetId = "666";


    @BeforeClass
    public void initialise() throws Exception {
        PostgresTest postgresTest = new PostgresTest(em, datasetId, logger);
        postgresTest.createDatabase();
    }


    @Test
    public void loadACsvIntoDb() {
        try {
            EntityTransaction tx = em.getTransaction();
            tx.begin();

            createDataset(datasetId, "Open-Data-small.csv", "Title");
            loadToTarget(datasetId);

            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        assertEquals(em.createQuery("SELECT ddp from DimensionalDataPoint ddp where ddp.dimensionalDataSet.dataResourceBean.dataResource = '666'").getResultList().size(), 276);
    }



    private void createDataset(String id, String filename, String title) {
        logger.debug("\n\n########   Start createDataset ###########\n\n");

        File inputFile = new File(new PostgresTest().getClass().getResource(filename).getPath());

        // todo this belongs as part of the csv 'import' function
        DataResource dataResource = new DataResource(id, title);
        DimensionalDataSet dimensionalDataSet = new DimensionalDataSet(title, dataResource);
        em.persist(dataResource);
        em.persist(dimensionalDataSet);

        new InputCSVParser().run(em, dimensionalDataSet, inputFile);

        em.flush();
        em.clear();
    }


    // NOTE: this step will not be required once the stage table stuff is removed
    private void loadToTarget(String id) throws Exception {
        logger.debug("\n\n########   Start loadToTarget ###########\n\n");

        DataResource dataResource = em.createQuery("SELECT d FROM DataResource d WHERE d.dataResource = :dsid", DataResource.class).setParameter("dsid", id).getSingleResult();

        List<DimensionalDataSet> dimensionalDataSets = em.createQuery("SELECT d FROM DimensionalDataSet d WHERE d.dataResourceBean = :dsid", DimensionalDataSet.class).setParameter("dsid", dataResource).getResultList();
        assertTrue(dimensionalDataSets.size() == 1);

        Long ddsId = dimensionalDataSets.get(0).getDimensionalDataSetId();

        // todo  sort the model here and remove state
        Editor editor = new Editor(id, ddsId);
        editor.setStatus(" loaded to target");

        new LoadToTarget().run(em, ddsId);
    }


}
