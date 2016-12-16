package main;

import org.apache.commons.io.FileUtils;
import play.Logger;
import services.InputCSVParser;
import uk.co.onsdigital.discovery.model.DataResource;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.io.File;
import java.util.UUID;


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
            DimensionalDataSet dimensionalDataSet = em.find(DimensionalDataSet.class, UUID.fromString(id));
            if (dimensionalDataSet == null) {
                DataResource resource = new DataResource(id, "title");
                em.persist(resource);
                dimensionalDataSet = new DimensionalDataSet(title, resource);
                dimensionalDataSet.setDimensionalDataSetId(UUID.fromString(id));
                em.persist(dimensionalDataSet);
            }
            new InputCSVParser(em).run(dimensionalDataSet, inputFile);

            em.flush();
            em.clear();

    }

}
