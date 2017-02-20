package configuration;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfo;
import org.flywaydb.core.api.MigrationInfoService;
import play.Logger;
import uk.co.onsdigital.discovery.constants.DbConstants;

import java.util.Map;

import static org.eclipse.persistence.config.EntityManagerProperties.*;

/**
 * Uses {@link Flyway} to upgrade the database to the latest version. DB scripts are expected to be on the classpath
 * (inherited from dp-dd-backend-model).
 */
public class DbMigrator {

    private static final Logger.ALogger logger = Logger.of(DbMigrator.class);

    private static DbMigrator singleton;

    private final Flyway flyway;

    /**
     * Constructor accepting a Flyway object to use.
     * See also {@link #create(Map, String...)}
     * @param flyway a fully-configured flyway object.
     */
    protected DbMigrator(Flyway flyway) {
        this.flyway = flyway;
    }

    /**
     * @return the {@link DbMigrator}, configured from system properties.
     */
    public static DbMigrator getMigrator() {
        if (singleton == null) {
            singleton = create(Configuration.getMigrationDatabaseParameters(), DbConstants.SQL_SCRIPTS_LOCATION);
        }
        return singleton;
    }

    /**
     * Creates a DbMigrator using the given jdb parameters and schema locations.
     * @param jdbcParameters map containing the following config values: #JDBC_URL, #JDBC_USER, #JDBC_PASSWORD.
     * @param scriptLocations The locations of the sql upgrade scripts. See 'locations' in: https://flywaydb.org/documentation/maven/migrate.
     * @return a new DbMigrator instance.
     */
    protected static DbMigrator create(Map<String, String> jdbcParameters, String... scriptLocations) {
        Flyway flyway = new Flyway();
        flyway.setDataSource(jdbcParameters.get(JDBC_URL), jdbcParameters.get(JDBC_USER), jdbcParameters.get(JDBC_PASSWORD));
        flyway.setLocations(scriptLocations);
        return new DbMigrator(flyway);
    }

    /**
     * Drops all objects in the database!
     */
    public void clean() {
        logger.warn("Cleaning the database!! - i.e. dropping all objects (tables etc).");
        flyway.clean();
    }

    /**
     * Runs all scripts necessary to bring the database up to date.
     */
    public void migrate() {
        MigrationInfoService info = flyway.info();
        logger.info("There are " + info.pending().length + " sql migration scripts to apply");
        for (MigrationInfo migrationInfo : info.pending()) {
            logger.info("\t" + migrationInfo.getScript());
        }
        int count = flyway.migrate();
        logger.info("Applied " + count + " scripts.");
    }

    /**
     *
     * @return the Flyway instance backing this migrator.
     */
    public Flyway getFlyway() {
        return flyway;
    }
}
