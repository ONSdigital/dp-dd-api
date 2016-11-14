package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

import play.Logger;
import services.InputCSVParser;

public class TimeHelper {
	/** The _logger. */
    private static final Logger.ALogger logger = Logger.of(InputCSVParser.class);
	public static CharSequence Q1 = "Q1";
	public static CharSequence Q2 = "Q2";
	public static CharSequence Q3 = "Q3";
	public static CharSequence Q4 = "Q4";
	public static CharSequence January = "Jan";
	public static CharSequence February = "Feb";
	public static CharSequence March = "Mar";
	public static CharSequence April = "Apr";
	public static CharSequence May = "May";
	public static CharSequence June = "Jun";
	public static CharSequence July = "Jul";
	public static CharSequence August = "Aug";
	public static CharSequence September = "Sep";
	public static CharSequence October = "Oct";
	public static CharSequence November = "Nov";
	public static CharSequence December = "Dec";
	public static CharSequence Y2000 = "2000";
	public static CharSequence Y2001 = "2001";
	public static CharSequence Y2002 = "2002";
	public static CharSequence Y2003 = "2003";
	public static CharSequence Y2004 = "2004";
	public static CharSequence Y2005 = "2005";
	public static CharSequence Y2006 = "2006";
	public static CharSequence Y2007 = "2007";
	public static CharSequence Y2008 = "2008";
	public static CharSequence Y2009 = "2009";
	public static CharSequence Y2010 = "2010";
	public static CharSequence Y2011 = "2011";
	public static CharSequence Y2012 = "2012";
	public static CharSequence Y2013 = "2013";
	public static CharSequence Y2014 = "2014";
	public static CharSequence Y2015 = "2015";
	public static CharSequence Y2016 = "2016";
	public static CharSequence Y2017 = "2017";
	public static CharSequence Y2018 = "2018";
	
	
	/* methods to calculate start and end time periods */

	public Date getStartDate(String dateString){
		String Month = "January";
		String Day = "1";
		String Year = "2016";
		
		if (dateString.contains(Q1)){
			Month = "January";
			Day = "1";
		}
		if (dateString.contains(Q2)){
			Month = "April";
			Day = "1";
		}
		if (dateString.contains(Q3)){
			Month = "July";
			Day = "1";
		}
		if (dateString.contains(Q4)){
			Month = "October";
			Day = "1";
		}
		
		if (dateString.contains(January)){
			Month = "January";
			Day = "1";
		}
		if (dateString.contains(February)){
			Month = "February";
			Day = "1";
		}
		if (dateString.contains(March)){
			Month = "March";
			Day = "1";
		}
		if (dateString.contains(April)){
			Month = "April";
			Day = "1";
		}
		if (dateString.contains(May)){
			Month = "May";
			Day = "1";
		}
		if (dateString.contains(June)){
			Month = "June";
			Day = "1";
		}
		if (dateString.contains(July)){
			Month = "July";
			Day = "1";
		}
		if (dateString.contains(August)){
			Month = "August";
			Day = "1";
		}
		if (dateString.contains(September)){
			Month = "September";
			Day = "1";
		}
		if (dateString.contains(October)){
			Month = "October";
			Day = "1";
		}
		if (dateString.contains(November)){
			Month = "November";
			Day = "1";
		}
		if (dateString.contains(December)){
			Month = "December";
			Day = "1";
		}
		
		
		if (dateString.contains(Y2000)){
			Year = "2000";
		}
		if (dateString.contains(Y2001)){
			Year = "2001";
		}
		if (dateString.contains(Y2002)){
			Year = "2002";
		}
		if (dateString.contains(Y2003)){
			Year = "2003";
		}
		if (dateString.contains(Y2004)){
			Year = "2004";
		}
		if (dateString.contains(Y2005)){
			Year = "2005";
		}
		if (dateString.contains(Y2006)){
			Year = "2006";
		}
		if (dateString.contains(Y2007)){
			Year = "2007";
		}
		if (dateString.contains(Y2008)){
			Year = "2008";
		}
		if (dateString.contains(Y2009)){
			Year = "2009";
		}
		if (dateString.contains(Y2010)){
			Year = "2010";
		}
		if (dateString.contains(Y2011)){
			Year = "2011";
		}
		if (dateString.contains(Y2012)){
			Year = "2012";
		}
		if (dateString.contains(Y2013)){
			Year = "2013";
		}
		if (dateString.contains(Y2014)){
			Year = "2014";
		}
		if (dateString.contains(Y2015)){
			Year = "2015";
		}
		if (dateString.contains(Y2016)){
			Year = "2016";
		}
		if (dateString.contains(Y2017)){
			Year = "2017";
		}
		if (dateString.contains(Y2018)){
			Year = "2018";
		}
		
		String newdate = Month + " " + Day + ", " + Year;
//		logger.info("newdate = "+newdate);
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMMM d, yyyy", Locale.ENGLISH);
		LocalDate ldate = LocalDate.parse(newdate, formatter);
		java.util.Date date = null;
		try {
			date = new SimpleDateFormat("yyyy-MM-dd").parse(ldate.toString());
		} catch (ParseException e) {
			e.printStackTrace();
		}
//		logger.info("Date = "+ date);

		return date;
	}
	
	public Date getEndDate(String dateString){
		String Month = "December";
		String Day = "31";
		String Year = "2016";
		
		if (dateString.contains(Q1)){
			Month = "March";
			Day = "31";
		}
		if (dateString.contains(Q2)){
			Month = "June";
			Day = "30";
		}
		if (dateString.contains(Q3)){
			Month = "September";
			Day = "30";
		}
		if (dateString.contains(Q4)){
			Month = "December";
			Day = "31";
		}
		
		if (dateString.contains(January)){
			Month = "January";
			Day = "31";
		}
		if (dateString.contains(February)){
			Month = "February";
			Day = "28";
		}
		if (dateString.contains(March)){
			Month = "March";
			Day = "31";
		}
		if (dateString.contains(April)){
			Month = "April";
			Day = "30";
		}
		if (dateString.contains(May)){
			Month = "May";
			Day = "31";
		}
		if (dateString.contains(June)){
			Month = "June";
			Day = "30";
		}
		if (dateString.contains(July)){
			Month = "July";
			Day = "31";
		}
		if (dateString.contains(August)){
			Month = "August";
			Day = "31";
		}
		if (dateString.contains(September)){
			Month = "September";
			Day = "30";
		}
		if (dateString.contains(October)){
			Month = "October";
			Day = "31";
		}
		if (dateString.contains(November)){
			Month = "November";
			Day = "31";
		}
		if (dateString.contains(December)){
			Month = "December";
			Day = "31";
		}
		
		
		if (dateString.contains(Y2000)){
			Year = "2000";
		}
		if (dateString.contains(Y2001)){
			Year = "2001";
		}
		if (dateString.contains(Y2002)){
			Year = "2002";
		}
		if (dateString.contains(Y2003)){
			Year = "2003";
		}
		if (dateString.contains(Y2004)){
			Year = "2004";
		}
		if (dateString.contains(Y2005)){
			Year = "2005";
		}
		if (dateString.contains(Y2006)){
			Year = "2006";
		}
		if (dateString.contains(Y2007)){
			Year = "2007";
		}
		if (dateString.contains(Y2008)){
			Year = "2008";
		}
		if (dateString.contains(Y2009)){
			Year = "2009";
		}
		if (dateString.contains(Y2010)){
			Year = "2010";
		}
		if (dateString.contains(Y2011)){
			Year = "2011";
		}
		if (dateString.contains(Y2012)){
			Year = "2012";
		}
		if (dateString.contains(Y2013)){
			Year = "2013";
		}
		if (dateString.contains(Y2014)){
			Year = "2014";
		}
		if (dateString.contains(Y2015)){
			Year = "2015";
		}
		if (dateString.contains(Y2016)){
			Year = "2016";
		}
		if (dateString.contains(Y2017)){
			Year = "2017";
		}
		if (dateString.contains(Y2018)){
			Year = "2018";
		}
		
		String newdate = Month + " " + Day + ", " + Year;
	//	logger.info("newdate = "+newdate);
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMMM d, yyyy", Locale.ENGLISH);
		LocalDate ldate = LocalDate.parse(newdate, formatter);
		java.util.Date date = null;
		try {
			date = new SimpleDateFormat("yyyy-MM-dd").parse(ldate.toString());
		} catch (ParseException e) {
			e.printStackTrace();
		}
//		logger.info("Date = "+ date);

		return date;
	}

}
