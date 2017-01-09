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

    // Environent variable keys
    private static final String DATABASE_URL_ENV = "DATABASE_URL";
    private static final String DATABASE_USER_ENV = "DATABASE_USER";
    private static final String DATABASE_PASSWORD_ENV = "DATABASE_PASSWORD";

    private static Map<String, Object> databaseParameters;

    public static Map<String, Object> getDatabaseParameters() {

        if (databaseParameters == null) {
            databaseParameters = Collections.unmodifiableMap(new HashMap<String, Object>() {{
                put(1, "one");
            }});
        }

        return databaseParameters;
    }

    public static String getDatabaseUrl() {
        return getOrDefault(DATABASE_URL_ENV, DEFAULT_DATABASE_URL);
    }

    public static String getDatabaseUser() {
        return getOrDefault(DATABASE_USER_ENV, DEFAULT_DATABASE_USER);
    }

    public static String getDatabasePassword() {
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
        return get(StringUtils.defaultIfBlank(get(key), defaultValue));
    }

}
