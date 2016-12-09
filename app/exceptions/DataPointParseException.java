package exceptions;

/**
 * Indicates an error occurred while mapping a datapoint JSON record received from the CSV splitter.
 */
public class DataPointParseException extends RuntimeException {
    public DataPointParseException(Throwable cause) {
        super(cause);
    }
}
