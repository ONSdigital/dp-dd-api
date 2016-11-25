package main;

import org.apache.commons.io.FileUtils;
import play.Logger;

import javax.persistence.*;
import java.io.File;
import static org.testng.Assert.fail;


public class PostgresTest {

    static Logger.ALogger logger = Logger.of(PostgresTest.class);


    public void createDatabase(EntityManager em) throws Exception {
        try {
            EntityTransaction tx = em.getTransaction();
            tx.begin();

            loadSomeData(em, "area_types.sql");
            loadSomeData(em, "2011gph.sql");
            loadSomeData(em, "2013admin.sql");

            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

    }

    private void loadSomeData(EntityManager em, String filename) throws Exception {
        File inputFile = new File(new PostgresTest().getClass().getResource(filename).getPath());
        String sqlScript = FileUtils.readFileToString(inputFile, "UTF-8");
        Query q = em.createNativeQuery(sqlScript);
        q.executeUpdate();
    }

}
