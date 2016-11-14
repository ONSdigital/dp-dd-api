package main;

import models.*;
import org.apache.commons.io.FileUtils;
import org.eclipse.persistence.tools.file.FileUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import play.Logger;
import services.CSVGenerator;
import services.InputCSVParser;
import services.LoadToTarget;

import javax.persistence.*;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.fail;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class SliceAndDiceTest {

    static EntityManagerFactory emf = Persistence.createEntityManagerFactory("OnslocalBOPU");
    static EntityManager em = emf.createEntityManager();
    static Logger.ALogger logger = Logger.of(SliceAndDiceTest.class);

    static String datasetId = "666";
    String outpileFilePath = "/logs/666.csv";


    @Test
    public void generateCsvWithoutFilter() throws Exception {
        assertEquals(276, FileUtils.readLines(generateCsv(new ArrayList<>())).size());
    }

    @Test
    public void generateCsvWithSingleFilter() throws Exception {
        List<DimensionFilter> dimensionFilters = new ArrayList<>();
        dimensionFilters.add(new DimensionFilter("NACE", "Other mining"));

        assertEquals(15, FileUtils.readLines(generateCsv(dimensionFilters)).size());
    }

    @Test
    public void generateCsvWithMulitpleFilters() throws Exception {
            List<DimensionFilter> dimensionFilters = new ArrayList<>();
            dimensionFilters.add(new DimensionFilter("NACE", "Other mining"));
            dimensionFilters.add(new DimensionFilter("Prodcom Elements", "Waste Products"));

            assertEquals(2, FileUtils.readLines(generateCsv(dimensionFilters)).size());
    }


    private File generateCsv(List<DimensionFilter> dimensionFilters) {
        try {
            EntityTransaction tx = em.getTransaction();
            tx.begin();
            em.merge(new PresentationType("CSV"));

            DimensionalDataSet dimensionalDataSet = em.createQuery("select dds from DimensionalDataSet as dds where dds.dataResourceBean.dataResource = :ddsId", DimensionalDataSet.class).setParameter("ddsId", datasetId).getSingleResult();
            new CSVGenerator().run(em, dimensionalDataSet, dimensionFilters, outpileFilePath);

            em.flush();
            em.clear();
            tx.commit();

            return new File(outpileFilePath);

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        return null;
    }









    @BeforeClass
    public static void createDatabase() throws Exception {
        try {
            EntityTransaction tx = em.getTransaction();
            tx.begin();

            createDataset(datasetId, "Open-Data-small.csv", "Title");
            loadAllData();
            loadToTarget(datasetId);

            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

    }

    public static void createDataset(String id, String filename, String title) {
        logger.debug("\n\n########   Start createDataset ###########\n\n");

        File inputFile = new File(new SliceAndDiceTest().getClass().getResource(filename).getPath());

        // todo this belongs as part of the csv 'import' function
        DataResource dataResource = new DataResource(id, title);
        DimensionalDataSet dimensionalDataSet = new DimensionalDataSet(title, dataResource);
        em.persist(dataResource);
        em.persist(dimensionalDataSet);

        new InputCSVParser().run(em, dimensionalDataSet, inputFile);

        em.flush();
        em.clear();
    }


    public static void loadAllData() throws Exception {
        logger.debug("\n\n########   Start loadAllData ###########\n\n");
        loadSomeData("/Users/allen/projects/ons/src/data-spike/sql/area_types.sql");
        loadSomeData("/Users/allen/projects/ons/src/data-spike/sql/2011gph.sql");
        loadSomeData("/Users/allen/projects/ons/src/data-spike/sql/2013admin.sql");
    }


    public static void loadSomeData(String filename) throws Exception {
        String sqlScript = FileUtils.readFileToString(new File(filename), "UTF-8");
        Query q = em.createNativeQuery(sqlScript);
        q.executeUpdate();
    }


    public static void loadToTarget(String id) throws Exception {
        logger.debug("\n\n########   Start loadToTarget ###########\n\n");

        DataResource dataResource = em.createQuery("SELECT d FROM DataResource d WHERE d.dataResource = :dsid", DataResource.class).setParameter("dsid", id).getSingleResult();

        List<DimensionalDataSet> dimensionalDataSets = em.createQuery("SELECT d FROM DimensionalDataSet d WHERE d.dataResourceBean = :dsid", DimensionalDataSet.class).setParameter("dsid", dataResource).getResultList();
        assertTrue(dimensionalDataSets.size() == 1);

        Long ddsId = dimensionalDataSets.get(0).getDimensionalDataSetId();

        // todo  sort the model here and remove state
        Editor editor = new Editor(id, ddsId);
        editor.setStatus(" loaded to target");
        LoadToTarget lot = new LoadToTarget(editor);

        lot.runJPA(em);
    }

}
