package utils;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import uk.gov.ons.wda.constants.CommonConstants;
//import uk.gov.ons.wda.constants.EntitiesConstants;

/**
 * The Class Utility.
 * PROBLEM Class description is missing.
 * 
 * @author naika5
 */
public class Utility {
	/** The _logger. */
	private static Logger _logger = LoggerFactory.getLogger(Utility.class.getName());
	/** The _class name. */
	private static String _className = Utility.class.getName();
	/** The Constant DATE_FORMAT. */
	public static final String DATE_FORMAT = "dd/MM/yyyy";
	/** The Constant DATE_TIME_FORMAT. */
	public static final String DATE_TIME_FORMAT = "dd/MM/yyyy HH:mm:ss";
	/** action status approved. */
	public static final String APPROVED = "Approved";
	/** action status delete. */
	public static final String DELETE = "Delete";

	/**
	 * Read major minor version.
	 * 
	 * @param strVersionNo the str version no
	 * @return the string[]
	 */
	public static String[] readMajorMinorVersion(final String strVersionNo) {
		return strVersionNo.split("\\.");
	}

	/**
	 * Assign major minor version.
	 * 
	 * @param majorVersionNo the major version no
	 * @param minorVersionNo the minor version no
	 * @param newStatus the new status
	 * @return the long[]
	 */
	public static long[] assignMajorMinorVersion(final long majorVersionNo, final long minorVersionNo, final String newStatus) {
		final long[] versions = new long[2];
		if (APPROVED.equalsIgnoreCase(newStatus)) {
			versions[0] = majorVersionNo + 1;
			versions[1] = 0;
		} else if (DELETE.equalsIgnoreCase(newStatus)) {
			// DO NOTHING WHEN THE STATUS IS DELETE...
			versions[0] = majorVersionNo;
			versions[1] = minorVersionNo;
		} else {
			versions[0] = majorVersionNo;
			versions[1] = minorVersionNo + 1;
		}
		return versions;
	}

	/**
	 * Assign major minor version.
	 * 
	 * @param versionNo the version no
	 * @param approved the approved
	 * @return the string
	 */
	public static String assignMajorMinorVersion(final String versionNo, final String approved) {
		String returnValue = "0.1";
		if ((versionNo != null) && !versionNo.isEmpty()) {
			final String[] versions = readMajorMinorVersion(versionNo);
			if (APPROVED.equalsIgnoreCase(approved)) {
				final Long tempValue = Long.valueOf(versions[0]) + 1;
				versions[0] = tempValue.toString();
				versions[1] = "0";
			} else if (DELETE.equalsIgnoreCase(approved)) {
				// DO NOTHING WHEN THE STATUS IS DELETE...
			} else {
				final Long tempValue = Long.valueOf(versions[1]) + 1;
				versions[1] = tempValue.toString();
			}
			returnValue = versions[0] + "." + versions[1];
		}
		return returnValue;
	}

	/**
	 * Parses the simple date.
	 * 
	 * @param format the format
	 * @param date the date
	 * @return the date
	 * @throws ParseException the parse exception
	 */
	public static Date parseSimpleDate(final String format, final String date) throws ParseException {
		return new SimpleDateFormat(format).parse(date);
	}

	/**
	 * Parses the simple date.
	 * 
	 * @param format the format
	 * @param sourceYear the source year
	 * @return the string
	 */
	public static String parseSimpleDate(final String format, final Date sourceYear) {
		final DateFormat df = new SimpleDateFormat(format);
		return df.format(sourceYear).toString();
	}

	/**
	 * Convert stringto date.
	 * 
	 * @param strDate "20/12/2012"
	 * @param returnDateRequired the return date required
	 * @return Date
	 */
	public static Date convertStringtoDate(final String strDate, final boolean returnDateRequired) {
		return convertStringtoDate(strDate, "dd/MM/yyyy", returnDateRequired);
	}

	/**
	 * Convert stringto date.
	 * 
	 * @param strDate "20/12/2012"
	 * @param dateFormat "dd/MM/yyyy"
	 * @param returnDateRequired the return date required
	 * @return Date
	 */
	public static Date convertStringtoDate(final String strDate, final String dateFormat, final boolean returnDateRequired) {
		Date convertedDate = null;
		try {
			convertedDate = parseSimpleDate(dateFormat, checkEmptyString(strDate));
		} catch (final ParseException e) {
			_logger.error("Utility.convertStringtoDate " + e);
			if (returnDateRequired) {
				convertedDate = Calendar.getInstance().getTime();
			}
		}
		return convertedDate;
	}

	/**
	 * Convert stringto date.
	 * 
	 * @param strDate "20/12/2012"
	 * @return Date
	 */
	public static Date convertStringtoDate(final String strDate) {
		return convertStringtoDate(strDate, true);
	}

	/**
	 * Convert stringto date.
	 * 
	 * @param strDate "20/12/2012"
	 * @param dateFormat "dd/MM/yyyy"
	 * @return Date
	 */
	public static Date convertStringtoDate(final String strDate, final String dateFormat) {
		return convertStringtoDate(strDate, dateFormat);
	}

	/**
	 * Check empty string.
	 * 
	 * @param checkString the check string
	 * @return the string
	 */
	public static String checkEmptyString(final String checkString) {
		if ((checkString != null) && !"".equals(checkString)) {
			return checkString.trim();
		}
		return null;
	}

	/**
	 * Converts the passed date to a string using format 'dd/MM/yyyy HH:mm:ss'.
	 * 
	 * @param date the date to be converted
	 * @return the string
	 */
	public static String convertDateTimetoString(final Date date) {
		String strDate = null;
		try {
			final SimpleDateFormat df = new SimpleDateFormat(DATE_TIME_FORMAT);
			strDate = df.format(date);
		} catch (final Exception e) {
			_logger.error("Utility.convertDateTimetoString " + e);
		}
		return strDate;
	}

	/**
	 * Converts the passed date to a string using format 'dd/MM/yyyy HH:mm'.
	 * 
	 * @param date the date to be converted
	 * @param dateFormat the date format
	 * @return the string
	 */
	public static String convertDateTimetoString(final Date date, final String dateFormat) {
		String strDate = null;
		try {
			final SimpleDateFormat df = new SimpleDateFormat(dateFormat);
			strDate = df.format(date);
		} catch (final Exception e) {
			_logger.error("Utility.convertDateTimetoString " + e);
		}
		return strDate;
	}

	/**
	 * Converts the passed date to a string using format 'dd/MM/yyyy'.
	 * 
	 * @param date the date to be converted
	 * @return the string
	 */
	public static String convertDateToString(final Date date) {
		String strDate = null;
		try {
			final SimpleDateFormat df = new SimpleDateFormat(DATE_FORMAT);
			strDate = df.format(date);
		} catch (final Exception e) {
			_logger.error("Utility.convertDateToString " + e);
		}
		return strDate;
	}

	/**
	 * Returns 0 for success.
	 * 
	 * @param shellScriptPath the shell script path
	 * @param fromDir the from dir
	 * @param destinationDir the destination dir
	 * @param zipFileName the zip file name
	 * @return the int
	 */
	@SuppressWarnings("finally")
	public static int callUnixShellScript(final String shellScriptPath, final String fromDir, final String destinationDir, final String zipFileName) {
		_logger.info(_className + " callUnixShellScript" + " shellScriptPath : " + shellScriptPath);
		_logger.info("FromDirectory : " + fromDir);
		_logger.info("destDirectory : " + destinationDir);
		_logger.info("zipFileName : " + zipFileName);
		int successCode = -1;
		final String[] cmdArray = { shellScriptPath, fromDir, destinationDir, zipFileName };
		try {
			_logger.info("calling script : " + shellScriptPath);
			final Process p = Runtime.getRuntime().exec(cmdArray);
			successCode = p.waitFor();
			_logger.info("script Executed with successCode : " + successCode);
		} catch (final InterruptedException e) {
			_logger.error("callUnixShellScript InterruptedException: " + e );
			return successCode;
		} catch (final IOException ioex) {
			_logger.error("callUnixShellScript IOException: " + ioex );
		} finally {
			return successCode;
		}
	}

	/**
	 * Call unix shell script.
	 * 
	 * @param shellScriptPath the shell script path
	 * @param objectParams the object params
	 * @return the int
	 */
	@SuppressWarnings("finally")
	public static int callUnixShellScript(final String shellScriptPath, final String objectParams) {
		_logger.info(_className + " callUnixShellScript" + " shellScriptPath : " + shellScriptPath);
		_logger.info("objectParams : " + objectParams);
		int successCode = -1;
		final String[] cmdArray = { shellScriptPath, objectParams };
		try {
			final Process p = Runtime.getRuntime().exec(cmdArray);
			p.waitFor();
			successCode = p.exitValue();
			_logger.info("successCode : " + successCode);
			System.out.println("successCode : " + successCode);
		} catch (final InterruptedException e) {
			_logger.error("callUnixShellScript InterruptedException: " + e );
			return successCode;
		} catch (final IOException ioex) {
			_logger.error("callUnixShellScript IOException: " + ioex );
		} finally {
			return successCode;
		}
	}

	/**
	 * Truncates String to either the (length -3) requested.
	 * If the last character is a space then that will also be removed.
	 * Three dots (...) are then added to the truncated string.
	 * 
	 * e.g.
	 * 
	 * String "abcdefghijklm" truncated to a length of 11 would return "abcdefgh...";
	 * 
	 * String "abcd efgh ijklm" truncated to a length of 11 would return "abcd efg...";
	 * 
	 * String "abcd ef gh ijklm" truncated to a length of 11 would return "abcd ef...";
	 * 
	 * If String length is less than the truncated length, String is not changed.
	 * If truncated length is less than 4, String is not changed.
	 * 
	 * @param string String to be truncated
	 * @param truncateToLength maximum length of String to be returned
	 * @return the string
	 */
	public static String truncateString(final String string, final int truncateToLength) {
		String truncatedString = string;
		try {
			if ((string != null) && !string.isEmpty() && (truncateToLength > 4)) {
				if (string.length() > truncateToLength) {
					truncatedString = string.substring(0, (truncateToLength - 3));
					/*
					 * If truncated String ends with a space " ".
					 * Remove it.
					 */
					if (truncatedString.endsWith(" ")) {
						truncatedString = truncatedString.substring(0, (truncatedString.length() - 1));
					}
					/*
					 * Add "..."
					 */
					truncatedString += "...";
				}
			}
		} catch (final Exception e) {
			/*
			 * Do nothing at present as String will just be returned
			 */
		}
		return truncatedString;
	}

	/**
	 * Match passed objects.
	 * 
	 * @param myValue own value
	 * @param otherValue other value
	 * @return true, if both match
	 */
	public static boolean matches(final Object myValue, final Object otherValue) {
		if (myValue == null) {
			if (otherValue != null) {
				return false;
			}
		} else if (!myValue.equals(otherValue)) {
			return false;
		}
		// Return true if no mismatch
		return true;
	}
	
	/**
	 * This method used by General Data Load. 
	 * This method will replace single quote with two double quotes.
	 * 
	 * @param str string which may contain double quote
	 * @return String, in which single double quote is replaced with two double quotes.
	 */	
	public static String replaceByDDQ(String str) {
		if(str != null && str.trim().length() > 0)
			return str.trim().replaceAll("\"", "\"\"");
		else
			return str;
	}
	
	/**
	 * This method used by General Data Load. 
	 * This method will removes trailing spaces, commmas and equal sign from the input string.
	 * 
	 * @param str string which may contain trailing spaces, commas, equal sign
	 * @return String, which doesn't contain comma, equal sign and trailing spaces.
	 */		
	public static String removeCommaEqual(String str) {
		if (str != null && str.trim().length() > 0) {
			return str.trim().replaceAll(",", "").replaceAll("=", "");
		} else {
			return str;
		}
	}
	
	/**
	 * This method used by General Data Load. 
	 * This method will removes trailing spaces, commmas and equal sign and apostrophe from the input string.
	 * 
	 * @param str string which may contain trailing spaces, commas, equal sign and apostrophe
	 * @return String, which doesn't contain comma, equal sign and trailing spaces.
	 */		
	public static String removeCommaEqualApostrophe(String str) {
		if (str != null && str.trim().length() > 0) {
			return str.trim().replaceAll(",", "").replaceAll("=", "").replaceAll("'", "");
		} else {
			return str;
		}
	}	
	/**
	 * This methods take number in string format returns as converted big decimal
	 * @param obsValue	String number
	 * @return	BigDecimal
	 */
	public static BigDecimal convertStringToBigDecimal(String obsValue) {
		if (obsValue.indexOf(".") != -1) {
			BigDecimal bd = new BigDecimal(obsValue);
			BigDecimal rounded = bd.setScale(1, BigDecimal.ROUND_HALF_EVEN);
			return rounded;
		} else {
			return new BigDecimal(obsValue);
		}
	}	
}
