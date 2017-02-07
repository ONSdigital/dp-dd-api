package exceptions;

/**
 * Indicates that processing a datapoint has failed in a non-recoverable way, meaning that the dataset cannot be loaded.
 * This is an error with the data itself and should not be retried, but reported to the author to correct their data.
 */
public class DatapointMappingException extends Exception {

    public DatapointMappingException(String message) {
        super(message);
    }

    public DatapointMappingException(String message, Throwable cause) {
        super(message, cause);
    }
}
