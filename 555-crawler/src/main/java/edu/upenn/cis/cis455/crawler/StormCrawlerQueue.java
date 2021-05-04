package edu.upenn.cis.cis455.crawler;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.sleepycat.je.ThreadInterruptedException;

import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import edu.upenn.cis.cis455.crawler.worker.WorkerServer;

/**
 * Queue that the storm crawler uses
 * @author Kevin Chen
 *
 */
public class StormCrawlerQueue {
	
	AtomicLong size = new AtomicLong();
	Map<String, DomainManager> domainManagerMap = new ConcurrentHashMap<String, DomainManager>();
	boolean pause = false;
	public boolean capacityReached = false;
	
	public StormCrawlerQueue() {
		size.set(WorkerServer.workerStorage.getQueueSize());
	}
	
	/**
	 * Get the domain specific queue based on a given domain
	 * @param domain
	 * @return Queue for given domain
	 */
	public DomainManager getDomainManager(String domain) {
		return domainManagerMap.get(domain);
	}
	
	/**
	 * Used to tell the queue to stop returning URLs on take
	 */
	public void pauseQueue() {
		pause = true;
	}
	
	/**
	 * Put a URL in the crawler queue
	 * @param url
	 */
	public void put(String url) {
		if (capacityReached || pause) {
			return;
		}

		URLInfo info = new URLInfo(url);
		
		// get manager corresponding to URL domain
		DomainManager manager = domainManagerMap.get(info.getDomain());
		
		// create if domain manager doesn't exist
		if (manager == null) {
			manager = new DomainManager(info.getDomain());
			domainManagerMap.put(info.getDomain(), manager);
			
			if (domainManagerMap.size() >= 1000000) {
				capacityReached = true;
			}
		}
		
		try {
			boolean putSuccess = WorkerServer.workerStorage.addQueueUrl(url, manager.getNextRequestDate());
				
			// only increase size if the url is allowed by robots.txt
			if (putSuccess) {
				size.incrementAndGet();
			}
		} catch (ThreadInterruptedException e) {
		}
	}
	
	public String take() {
		if (pause || size.get() == 0) {
			return null;
		}

		try {
			size.decrementAndGet();
			String url = WorkerServer.workerStorage.takeQueueUrl();
			
			if (url == null) {
				size.incrementAndGet();
				return null;
			}
			
			URLInfo info = new URLInfo(url);
			DomainManager manager = domainManagerMap.get(info.getDomain());
			
			if (manager == null) {
				manager = new DomainManager(info.getDomain());
				domainManagerMap.put(info.getDomain(), manager);
			}
			
			if (manager.getNextRequestDate().after(new Date())) {
				WorkerServer.workerStorage.addQueueUrl(url, manager.getNextRequestDate());
				size.incrementAndGet();
				return null;
			} else {
				manager.madeRequest();
				return url;
			}
			
		} catch (ThreadInterruptedException e) {
		}
		
		return null;
	}
}
