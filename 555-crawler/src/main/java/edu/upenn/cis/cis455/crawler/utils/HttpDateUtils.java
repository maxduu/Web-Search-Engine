package edu.upenn.cis.cis455.crawler.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Class to handle http related date operations
 * @author Kevin Chen
 *
 */
public class HttpDateUtils {
	
	// various date formats used for http requests
	
	/**
	 * Return current date based on HTTP guidelines
	 * @return String with current GMT time in HTTP acceptable format
	 */
	public static String getCurrentGMT() {
	    SimpleDateFormat dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
	    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
	    return dateFormat.format(new Date());
	}
	
	/**
	 * Formats a given date object to string
	 * @param date
	 * @return Date string usable by HTTP
	 */
	public static String formatGMT(Date date) {
	    SimpleDateFormat dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
	    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
	    return dateFormat.format(date);
	}
	
	/**
	 * Parses a date string given by an HTTP request to a date object
	 * @param dateString
	 * @return Date object
	 * @throws ParseException
	 */
	public static Date parseHttpString(String dateString) throws ParseException {
	    SimpleDateFormat dateFormat1 = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
		SimpleDateFormat dateFormat2 = new SimpleDateFormat("EEEE, dd-MMM-yy HH:mm:ss z", Locale.US);
		SimpleDateFormat dateFormat3 = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy", Locale.US);
		
		Date date = null;
		try {
			date = dateFormat1.parse(dateString);
			return date;
		} catch (ParseException e) {}
		
		try {
			date = dateFormat2.parse(dateString);
			return date;
		} catch (ParseException e) {}
		
		date = dateFormat3.parse(dateString);
		return date;
	}
}
