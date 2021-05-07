package edu.upenn.cis.cis455.crawler.utils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.HttpsURLConnection;

/**
 * Class used to parse robots.txt files
 * @author Kevin Chen
 *
 */
public class RobotsTxtUtils {	
	
	String robot;
	Set<String> allowed;
	Set<String> disallowed;
	String domain;
	double delay;
	
	public RobotsTxtUtils(String robot, String domain) {
		this.robot = robot;
		this.allowed = new HashSet<String>();
		this.disallowed = new HashSet<String>();
		this.delay = 0;
		this.domain = domain;
		parseRobotsTxt();
	}
	
	private String getRegex(String template) {
		String regExSpecialChars = "<([{\\^-=$!|]})?+.>";
		String regExSpecialCharsRE = regExSpecialChars.replaceAll( ".", "\\\\$0");
		Pattern reCharsREP = Pattern.compile( "[" + regExSpecialCharsRE + "]");

		Matcher m = reCharsREP.matcher(template);
	    template = m.replaceAll("\\\\$0");
		
		template = template.replaceAll("\\*", ".*");
				
		if (!template.endsWith("$")) {
			template += ".*";
		} else {
			template = template.substring(0, template.length() - 2);
		}
		
		return template;
	}
	
	/**
	 * See if a filePath is listed under Allow: filePath
	 * @param filePath
	 * @return
	 */
	public boolean isAllowed(String filePath) {
		for (String path : allowed) {
			if (filePath.matches(getRegex(path))) {
				return true;
			}
		}
		return false;
	}

	/**
	 * See if a filePath is listed under Disallow: filePath
	 * @param filePath
	 * @return
	 */
	public boolean isDisallowed(String filePath) {
		for (String path : disallowed) {
			if (filePath.matches(getRegex(path))) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Get the Crawl-delay
	 * @return
	 */
	public double getDelay() {
		return delay;
	}
	
	private String getRobotsTxt(String domain) {
		// construct url and connection to get the content
		URL urlObj;
		try {
			urlObj = new URL(domain + "/robots.txt");
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return "";
		}
		
    	HttpURLConnection urlConnection;

    	try {
			if (domain.startsWith("https")) {
				urlConnection = (HttpsURLConnection) urlObj.openConnection();
			} else {
				urlConnection = (HttpURLConnection) urlObj.openConnection();
			}
    	} catch(IOException e) {
    		e.printStackTrace();
    		return "";
    	}
    	
		urlConnection.setRequestProperty("User-Agent", "cis455crawler");
		urlConnection.setConnectTimeout(5000);
		urlConnection.setReadTimeout(5000);
		
		try {
			BufferedInputStream in = new BufferedInputStream(urlConnection.getInputStream());
		    return new String(in.readAllBytes());
		} catch (SocketTimeoutException e) {
			disallowed.add("/");
			return "";
		} catch (IOException e) {
			// case when no robots.txt exists
			return "";
		}
	}

	/**
	 * Helper function to parse the robots.txt file
	 */
	private void parseRobotsTxt() {
		// split the robots.txt file by lines
		String content = getRobotsTxt(domain);
    	String[] lineSplit = content.split("\n");
    	String currentUserAgent = "";
    	boolean parseAgent = false;

    	for (String line : lineSplit) {
    		
    		// blank line means new record
    		if (line.isBlank()) {
    			parseAgent = false;
    			continue;
    		}
    		
    		// handle comments in the beginning or middle of line
    		if (line.startsWith("#")) {
    			continue;
    		}
    		if (line.contains("#")) {
    			line = line.substring(0, line.indexOf("#"));
    		}
    		
    		// got User-agent keyword - check if this user-agent is a better "fit"
    		if (line.startsWith("User-agent:") && !parseAgent) {
    			// get part after the colon
    			String nextAgent = line.substring(line.indexOf(":") + 1).trim();
    			if (currentUserAgent.equals("") && (nextAgent.equals("*") || nextAgent.equals(robot))) {
					parseAgent = true;
					currentUserAgent = nextAgent;
					delay = 0;
					disallowed.clear();
					allowed.clear();
				} else if (currentUserAgent.equals("*") && nextAgent.equals(robot)) {
					parseAgent = true;
					currentUserAgent = nextAgent;
					delay = 0;
					disallowed.clear();
					allowed.clear();
				} else {
					parseAgent = false;
				}
    		// got disallow keyword
    		} else if (line.startsWith("Disallow:") && parseAgent) {
    			String disallow = line.substring(line.indexOf(":") + 1).trim();
    			disallowed.add(disallow);
    		// got allow keyword
    		} else if (line.startsWith("Allow:") && parseAgent) {
    			String allow = line.substring(line.indexOf(":") + 1).trim();
    			allowed.add(allow);
    		// got crawl delay keyword
    		} else if (line.startsWith("Crawl-delay:") && parseAgent) {
    			String delayString = line.substring(line.indexOf(":") + 1).trim();
    			delay = Double.parseDouble(delayString);
    		}
    	} 	
	}
}
