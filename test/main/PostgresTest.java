package main;

import models.DataResource;
import models.DimensionalDataSet;
import models.Editor;
import org.apache.commons.io.FileUtils;
import play.Logger;
import services.InputCSVParser;
import services.LoadToTarget;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.io.File;
import java.util.List;

import static org.testng.Assert.assertTrue;


public class PostgresTest {

    static Logger.ALogger logger = Logger.of(PostgresTest.class);


    public void createDatabase(EntityManager em) throws Exception {
        loadSomeData(em, "area_types.sql");
        loadSomeData(em, "2011gph_small.sql");
        loadSomeData(em, "2013admin_small.sql");
    }

    private void loadSomeData(EntityManager em, String filename) throws Exception {
        File inputFile = new File(new PostgresTest().getClass().getResource(filename).getPath());
        String sqlScript = FileUtils.readFileToString(inputFile, "UTF-8");
        Query q = em.createNativeQuery(sqlScript);
        q.executeUpdate();
    }


    public void createDataset(EntityManager em, String id, String filename, String title) throws Exception {
            logger.debug("\n\n########   Start createDataset ###########\n\n");

            File inputFile = new File(new PostgresTest().getClass().getResource(filename).getPath());

            // todo this belongs as part of the csv 'import' function
            DataResource dataResource = em.find(DataResource.class, "666");
            if (dataResource == null) {
                dataResource = new DataResource(id, title);
                em.persist(dataResource);
            }
            List<DimensionalDataSet> dimensionalDataSetList = em.createQuery("SELECT dds FROM DimensionalDataSet dds WHERE dds.dataResourceBean = :drb", DimensionalDataSet.class).setParameter("drb", dataResource).getResultList();
            DimensionalDataSet dimensionalDataSet;
            if (dimensionalDataSetList.isEmpty()) {
                dimensionalDataSet = new DimensionalDataSet(title, dataResource);
                em.persist(dimensionalDataSet);
            } else {
                dimensionalDataSet = dimensionalDataSetList.get(0);
            }
            new InputCSVParser().run(em, dimensionalDataSet, inputFile);

            em.flush();
            em.clear();

            loadToTarget(em, id);
    }


    // NOTE: this step will not be required once the stage table stuff is removed
    private void loadToTarget(EntityManager em, String id) throws Exception {
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
