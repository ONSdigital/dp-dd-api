package configuration;

import exceptions.DatasetStatusException;
import models.DatasetStatus;
import org.eclipse.persistence.config.SessionCustomizer;
import org.eclipse.persistence.sessions.Session;
import org.eclipse.persistence.sessions.SessionEvent;
import org.eclipse.persistence.sessions.SessionEventAdapter;
import org.eclipse.persistence.sessions.UnitOfWork;
import org.postgresql.util.PSQLException;
import play.Logger;
import services.InputCSVParserV3;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Capable of running arbitrary sql scripts after session login.
 * This class is intended to be a temporary fix to allow us to import multiple sql files until we have a better form of db change management.
 * See trello ticket #330
 */
public class SqlImporter {

    private static final Logger.ALogger logger = Logger.of(SqlImporter.class);

    public static final String TIME = "/sql/time.sql";
    public static final String _2011STATH = "/sql/2011STATH.sql";
    public static final String _2011STATH_small = "/sql/2011STATH_small.sql";
    public static final String _2013WARDH = "/sql/2013WARDH.sql";
    public static final String COICOP = "/sql/CL_0000641_COICOP_Special_Aggregate.sql";
    public static final String NACE = "/sql/CL_0001480_NACE.sql";
    public static final String PRODCOM_ELEMENTS = "/sql/CL_0000737_Prodcom_Elements.sql";


    private static final String[] FILES_TO_IMPORT = {TIME, _2011STATH_small, _2013WARDH, COICOP, NACE, PRODCOM_ELEMENTS};

    public void importSql(EntityManager entityManager) {
        for (String filename : FILES_TO_IMPORT) {
            logger.info("About to import " + filename);
            EntityTransaction tx = entityManager.getTransaction();
            tx.begin();
            try {
                Session session = entityManager.unwrap(Session.class);
                int count = importSql(session, filename);
                tx.commit();
                logger.info("Finished importing " + filename + ": " + count + " statements executed");
            } catch (Exception ex) {
                logger.error("Unable to import sql file " + filename, ex);
                tx.rollback();
            }
        }
    }

    private int importSql(Session unitOfWork, String fileName) {
        int linesExecuted = 0;
        try(BufferedReader bufferedReader = openFile(fileName);) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                int commentIdx = line.indexOf("--");
                if (commentIdx > 0) {
                    line = line.substring(0, commentIdx);
                }
                line = line.trim();
                if (!line.isEmpty()) {
                    logger.debug("Running: {}", line);
                    unitOfWork.executeNonSelectingSQL(line);
                    linesExecuted++;
                }
            }
        } catch (IOException | RuntimeException e) {
            logger.error("Unable to import sql file " + fileName, e);
        }
        return linesExecuted;
    }

    BufferedReader openFile(String filename) {
        return new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream(filename)));
    }
}
