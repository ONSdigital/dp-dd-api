package main;

import models.*;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import play.Logger;
import services.CSVGenerator;

import javax.persistence.*;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class SliceAndDiceTest {

    static EntityManagerFactory emf = Persistence.createEntityManagerFactory("OnslocalBOPU");
    static EntityManager em = emf.createEntityManager();
    static Logger.ALogger logger = Logger.of(SliceAndDiceTest.class);

    static String datasetId = "666";
    String outpileFilePath = "/logs/666.csv";


    @BeforeClass
    public void initialise() throws Exception {
        PostgresTest postgresTest = new PostgresTest(em, datasetId, logger);
        postgresTest.createDatabase();
    }

    @Test
    public void generateCsvWithoutFilter() throws Exception {
        assertEquals(FileUtils.readLines(generateCsv(new ArrayList<>())).size(), 276);
    }

    @Test
    public void generateCsvWithSingleFilter() throws Exception {
        List<DimensionFilter> dimensionFilters = new ArrayList<>();
        dimensionFilters.add(new DimensionFilter("NACE", "Other mining"));

        assertEquals(FileUtils.readLines(generateCsv(dimensionFilters)).size(), 15);
    }

    @Test
    public void generateCsvWithMulitpleFilters() throws Exception {
            List<DimensionFilter> dimensionFilters = new ArrayList<>();
            dimensionFilters.add(new DimensionFilter("NACE", "Other mining"));
            dimensionFilters.add(new DimensionFilter("Prodcom Elements", "Waste Products"));

            assertEquals(FileUtils.readLines(generateCsv(dimensionFilters)).size(), 2);
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



}
