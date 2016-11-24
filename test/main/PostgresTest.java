package main;

import models.*;
import org.apache.commons.io.FileUtils;
import play.Logger;
import services.InputCSVParser;
import services.LoadToTarget;

import javax.persistence.*;
import java.io.File;
import java.util.List;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class PostgresTest {

    static EntityManager em;
    static String datasetId;
    static Logger.ALogger logger;

    public PostgresTest() {}

    public PostgresTest(EntityManager em, String datasetId, Logger.ALogger logger) {
        this.em = em;
        this.datasetId = datasetId;
        this.logger = logger;
    }

    public void createDatabase() throws Exception {
        try {
            EntityTransaction tx = em.getTransaction();
            tx.begin();

            loadAllData();
            createDataset(datasetId, "Open-Data-small.csv", "Title");
            loadToTarget(datasetId);

            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

    }

    public void loadAllData() throws Exception {
        logger.debug("\n\n########   Start loadAllData ###########\n\n");
        loadSomeData("area_types.sql");
        loadSomeData("2011gph.sql");
        loadSomeData("2013admin.sql");
    }


    public void loadSomeData(String filename) throws Exception {
        File inputFile = new File(new PostgresTest().getClass().getResource(filename).getPath());
        String sqlScript = FileUtils.readFileToString(inputFile, "UTF-8");
        Query q = em.createNativeQuery(sqlScript);
        q.executeUpdate();
    }


    public void createDataset(String id, String filename, String title) {
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


    public void loadToTarget(String id) throws Exception {
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
