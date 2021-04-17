package edu.upenn.cis.cis455.crawler.utils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import javax.net.ssl.HttpsURLConnection;

/**
 * Class used to parse robots.txt files
 * @author Kevin Chen
 *
 */
public class RobotsTxtUtils {	
	
	String robot;
	String domain;
	Set<String> allowed;
	Set<String> disallowed;
	int delay;
	
	public RobotsTxtUtils(String robot, String domain) {
		this.robot = robot;
		this.domain = domain;
		this.allowed = new HashSet<String>();
		this.disallowed = new HashSet<String>();
		this.delay = 0;
		parseRobotsTxt();
	}
	
	/**
	 * See if a filePath is listed under Allow: filePath
	 * @param filePath
	 * @return
	 */
	public boolean isAllowed(String filePath) {
		for (String path : allowed) {
			if (filePath.startsWith(path)) {
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
			if (filePath.startsWith(path)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Get the Crawl-delay
	 * @return
	 */
	public int getDelay() {
		return delay;
	}

	/**
	 * Helper function to parse the robots.txt file
	 */
	private void parseRobotsTxt() {
		
		// construct url and connection to get the content
		URL urlObj;
		try {
			urlObj = new URL(domain + "/robots.txt");
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return;
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
    		return;
    	}
    	
		urlConnection.setRequestProperty("User-Agent", "cis455crawler");
		
		String content = "";
		try {
			BufferedInputStream in = new BufferedInputStream(urlConnection.getInputStream());
		    content = new String(in.readAllBytes());
		} catch (IOException e) {
			// case when no robots.txt exists
			return;
		}
    	
		// split the robots.txt file by lines
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
    			delay = Integer.parseInt(delayString);
    		}
    	} 	
	}
}
