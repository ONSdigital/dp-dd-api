package main;

import org.apache.commons.io.FileUtils;
import play.Logger;

import javax.persistence.*;
import java.io.File;

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

            loadStandingData();

            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

    }

    public void loadStandingData() throws Exception {
        logger.debug("\n\n########   Start loadStandingData ###########\n\n");
        loadSomeData("area_types.sql");
        loadSomeData("2011gph.sql");
        loadSomeData("2013admin.sql");
    }


    private void loadSomeData(String filename) throws Exception {
        File inputFile = new File(new PostgresTest().getClass().getResource(filename).getPath());
        String sqlScript = FileUtils.readFileToString(inputFile, "UTF-8");
        Query q = em.createNativeQuery(sqlScript);
        q.executeUpdate();
    }




}
