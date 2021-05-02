package edu.upenn.cis.cis455.crawler;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import edu.upenn.cis.cis455.crawler.utils.RobotsTxtUtils;
import edu.upenn.cis.cis455.crawler.utils.URLInfo;

/**
 * Queues based on domains that the crawler queue stores
 * @author Kevin Chen
 *
 */
public class DomainQueue {

	List<String> urls = new ArrayList<String>();
	String domain;
	RobotsTxtUtils robotUtil;
	Date lastRequest;
	
	public DomainQueue(String domain) {
		this.domain = domain;
		this.robotUtil = new RobotsTxtUtils("cis455crawler", domain);
		this.lastRequest = new Date();
	}
	
	/**
	 * Put a url into the domain queue
	 * @param url
	 * @return If the put was successful, will not be successful if robots.txt disallows the url
	 */
	public boolean put(String url) {
		if (checkDisallowed(url)) {
			return false;
		}
		urls.add(url);
		return true;
	}
	
	/**
	 * Check if the domain queue contains a url
	 * @param url
	 * @return
	 */
	public boolean contains(String url) {
		return urls.contains(url);
	}
	
	public int size() {
		return urls.size();
	}
	
	/**
	 * Get the crawler delay that is remaining
	 * @return
	 */
	public double getDelayRemaining() {
		Date d = new Date();
		return robotUtil.getDelay() * 1000 - (d.getTime() - lastRequest.getTime());
	}
	
	/**
	 * See if a url is disallowed by robots.txt for the domain
	 * @param url
	 * @return
	 */
	public boolean checkDisallowed(String url) {
		URLInfo urlInfo = new URLInfo(url);
		return robotUtil.isDisallowed(urlInfo.getFilePath()) && !robotUtil.isAllowed(urlInfo.getFilePath());
	}
	
	/**
	 * Take an url from the queue
	 * @return
	 */
	public String take() {
		lastRequest = new Date();
		String url = urls.get(0);
		urls.remove(0);
		return url;
	}
	
	/**
	 * Check if the queue is empty
	 * @return
	 */
	public boolean isEmpty() {
		return urls.isEmpty();
	}
	
}
