package configuration;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Convenience class to get configuration values from {@link System#getProperty(String)} or gracefully fall back to {@link System#getenv()}.
 */
public class Configuration {

    // Default values
    private static final String DEFAULT_DATABASE_URL = "jdbc:postgresql://localhost:5432/data_discovery";
    private static final String DEFAULT_DATABASE_USER = "data_discovery";
    private static final String DEFAULT_DATABASE_PASSWORD = "password";

    private static final String DEFAULT_KAFKA_ADDRESS = "localhost:9092";
    private static final String DEFAULT_KAFKA_CONSUMER_TOPIC = "test";
    private static final String DEFAULT_KAFKA_CONSUMER_GROUP = "database-loader";

    // Environment variable keys
    private static final String DATABASE_URL_ENV = "DATABASE_URL";
    private static final String DATABASE_USER_ENV = "DATABASE_USER";
    private static final String DATABASE_PASSWORD_ENV = "DATABASE_PASSWORD";

    private static final String KAFKA_ADDRESS_ENV = "KAFKA_ADDR";
    private static final String KAFKA_CONSUMER_TOPIC_ENV = "KAFKA_CONSUMER_TOPIC";
    private static final String KAFKA_CONSUMER_GROUP_ENV = "KAFKA_CONSUMER_GROUP";

    // Lazy loaded static in memory cache for database parameters.
    private static Map<String, Object> databaseParameters;

    public static Map<String, Object> getDatabaseParameters() {

        if (databaseParameters == null) {
            databaseParameters = Collections.unmodifiableMap(new HashMap<String, Object>() {{
                put("javax.persistence.jdbc.url", getDatabaseUrl());
                put("javax.persistence.jdbc.user", getDatabaseUser());
                put("javax.persistence.jdbc.password", getDatabasePassword());
            }});
        }

        return databaseParameters;
    }

    public static String getKafkaAddress() {
        return getOrDefault(KAFKA_ADDRESS_ENV, DEFAULT_KAFKA_ADDRESS);
    }

    public static String getKafkaConsumerTopic() {
        return getOrDefault(KAFKA_CONSUMER_TOPIC_ENV, DEFAULT_KAFKA_CONSUMER_TOPIC);
    }

    public static String getKafkaConsumerGroup() {
        return getOrDefault(KAFKA_CONSUMER_GROUP_ENV, DEFAULT_KAFKA_CONSUMER_GROUP);
    }

    private static String getDatabaseUrl() {
        return getOrDefault(DATABASE_URL_ENV, DEFAULT_DATABASE_URL);
    }

    private static String getDatabaseUser() {
        return getOrDefault(DATABASE_USER_ENV, DEFAULT_DATABASE_USER);
    }

    private static String getDatabasePassword() {
        return getOrDefault(DATABASE_PASSWORD_ENV, DEFAULT_DATABASE_PASSWORD);
    }

    /**
     * Gets a configuration value from {@link System#getProperty(String)}, falling back to {@link System#getenv()}
     * if the property comes back blank.
     *
     * @param key The configuration value key.
     * @return A system property or, if that comes back blank, an environment value.
     */
    public static String get(String key) {
        return StringUtils.defaultIfBlank(System.getProperty(key), System.getenv(key));
    }

    /**
     * Gets a configuration value from {@link System#getProperty(String)}, falling back to {@link System#getenv()}
     * if the property comes back blank, then falling back to the default value.
     *
     * @param key          The configuration value key.
     * @param defaultValue The default to use if neither a property nor an environment value are present.
     * @return The result of {@link #get(String)}, or <code>defaultValue</code> if that result is blank.
     */
    public static String getOrDefault(String key, String defaultValue) {
        return StringUtils.defaultIfBlank(get(key), defaultValue);
    }

}
