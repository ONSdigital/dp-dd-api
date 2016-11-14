/**
 * 
 */
package exceptions;

/**
 * This Exception is thrown when the Generic Load CSV file fails validation.
 * 
 * @author mashep
 * 
 */
public class CSVValidationException extends RuntimeException {
	private static final long serialVersionUID = 3241302147134725358L;

	/**
	 * 
	 */
	public CSVValidationException() {
	}

	/**
	 * @param arg0
	 */
	public CSVValidationException(String arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 */
	public CSVValidationException(Throwable arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public CSVValidationException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}
}
