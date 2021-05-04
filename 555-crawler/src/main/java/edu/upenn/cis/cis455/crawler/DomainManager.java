package edu.upenn.cis.cis455.crawler;

import java.util.Date;

import edu.upenn.cis.cis455.crawler.utils.RobotsTxtUtils;
import edu.upenn.cis.cis455.crawler.utils.URLInfo;

/**
 * Queues based on domains that the crawler queue stores
 * @author Kevin Chen
 *
 */
public class DomainManager {

	RobotsTxtUtils robotUtil;
	Date lastRequest;
	String domain;
	
	public DomainManager(String domain) {
		this.domain = domain;
		this.robotUtil = new RobotsTxtUtils("cis455crawler", domain);
		this.lastRequest = new Date();
	}
	
	/**
	 * Get the crawler delay that is remaining
	 * @return
	 */
	public Date getNextRequestDate() {
		Date d = new Date();
		long offset = Math.round(robotUtil.getDelay() * 1000) - (d.getTime() - lastRequest.getTime());
		
		if (offset > 0)
			return new Date(d.getTime() + offset);
		else
			return d;
	}
	
	public void madeRequest() {
		lastRequest = new Date();
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

	@Override
	public String toString() {
		return domain;
	}
	
}
