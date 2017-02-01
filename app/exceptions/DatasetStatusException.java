package exceptions;

/**
 * Indicates that processing a dataset status has failed.
 */
public class DatasetStatusException extends Exception {

    public DatasetStatusException(Exception ex) {
        super(ex);
    }

    public DatasetStatusException(String message) {
        super(message);
    }
}
