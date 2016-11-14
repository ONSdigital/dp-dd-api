/**
 * 
 */
package exceptions;

/**
 * A runtime exception specifically in use by fileload webservices
 * 
 * @author ladapod
 *         Created: 13 Jan 2013 16:37:25
 */
public class GLLoadException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2753184627041760501L;

	/**
	 * Default Constructor
	 */
	public GLLoadException() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * Default Constructor
	 * 
	 * @param arg0
	 */
	public GLLoadException(String arg0) {
		super(arg0);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Default Constructor
	 * 
	 * @param arg0
	 */
	public GLLoadException(Throwable arg0) {
		super(arg0);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Default Constructor
	 * 
	 * @param arg0
	 * @param arg1
	 */
	public GLLoadException(String arg0, Throwable arg1) {
		super(arg0, arg1);
		// TODO Auto-generated constructor stub
	}
}
