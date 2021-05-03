package edu.upenn.cis.cis455.crawler;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import edu.upenn.cis.cis455.crawler.utils.RobotsTxtUtils;
import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import edu.upenn.cis.cis455.crawler.worker.WorkerServer;
import edu.upenn.cis.cis455.storage.Domain;

/**
 * Queues based on domains that the crawler queue stores
 * @author Kevin Chen
 *
 */
public class DomainQueue {

	RobotsTxtUtils robotUtil;
	Date lastRequest;
	long domainId;
	
	public DomainQueue(Domain domain) {
		this.domainId = domain.id;
		this.robotUtil = new RobotsTxtUtils("cis455crawler", domain.robotsTxtContent);
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
		return WorkerServer.workerStorage.addQueueUrl(url, domainId);
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
		String url = WorkerServer.workerStorage.takeQueueUrl(domainId);
		if (url != null) {
			lastRequest = new Date();
		}
		return url;
	}
	
}
