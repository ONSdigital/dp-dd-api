package exceptions;

/**
 * Indicates that processing a dataset status has failed in a non-recoverable way.
 */
public class DatasetStatusException extends Exception {

    public DatasetStatusException(String message) {
        super(message);
    }
}
