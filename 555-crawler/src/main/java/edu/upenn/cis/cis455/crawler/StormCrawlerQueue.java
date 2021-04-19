package edu.upenn.cis.cis455.crawler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import edu.upenn.cis.cis455.crawler.worker.WorkerServer;

/**
 * Queue that the storm crawler uses
 * @author Kevin Chen
 *
 */
public class StormCrawlerQueue {
	
	int size = 0;
	int currIndex = 0;
	List<DomainQueue> domainQueues = new ArrayList<DomainQueue>();
	Map<String, DomainQueue> domainQueueMap = new HashMap<String, DomainQueue>();
	boolean pause = false;
	
	/**
	 * Get the domain specific queue based on a given domain
	 * @param domain
	 * @return Queue for given domain
	 */
	synchronized DomainQueue getDomainQueue(String domain) {
		return domainQueueMap.get(domain);
	}
	
	/**
	 * Used to tell the queue to stop returning URLs on take
	 */
	void pauseQueue() {
		pause = true;
	}
	
	/**
	 * Put a URL in the crawler queue
	 * @param url
	 */
	public synchronized void put(String url) {
		URLInfo info = new URLInfo(url);
		
		// get queue corresponding to URL domain
		DomainQueue q = domainQueueMap.get(info.getDomain());
		
		// create if domain queue doesn't exist
		if (q == null) {
			q = new DomainQueue(info.getDomain());
			domainQueueMap.put(info.getDomain(), q);
			domainQueues.add(q);
		}
		
		if (!q.contains(url)) {
			boolean putSuccess = q.put(url);
			
			// only increase size if the url is allowed by robots.txt
			if (putSuccess) {
				size++;
			}
		}
	}
	
	synchronized String take() throws InterruptedException {
		int startIndex = currIndex;

		while (true) {
			// if queue is empty, wait
			if (size == 0 || pause) {
				return null;
			}
			
			// wrap around if index gets too large
			currIndex = currIndex % domainQueues.size();
			
			if (!domainQueues.get(currIndex).isEmpty()) {
				
				// check if the domain queue is in a crawler delay
				if (domainQueues.get(currIndex).getDelayRemaining() <= 0) {
					WorkerServer.crawler.setWorking(true);
					size--;
					String url = domainQueues.get(currIndex).take();
					currIndex++;
					return url;
				}
			}
			
			currIndex++;
			
			// if we've gone to all indexes and not found anything, return null
			if (currIndex == startIndex) {
				return null;
			}
		}
	}
}
