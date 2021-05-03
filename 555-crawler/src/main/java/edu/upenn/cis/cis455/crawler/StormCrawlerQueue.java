package edu.upenn.cis.cis455.crawler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.sleepycat.je.ThreadInterruptedException;

import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import edu.upenn.cis.cis455.crawler.worker.WorkerServer;
import edu.upenn.cis.cis455.storage.Domain;

/**
 * Queue that the storm crawler uses
 * @author Kevin Chen
 *
 */
public class StormCrawlerQueue {
	
	AtomicLong size = new AtomicLong();
	AtomicInteger currIndex = new AtomicInteger();
	AtomicInteger domainCount = new AtomicInteger();
	Map<Integer, DomainQueue> domainQueues = new ConcurrentHashMap<Integer, DomainQueue>();
	Map<String, DomainQueue> domainQueueMap = new ConcurrentHashMap<String, DomainQueue>();
	boolean pause = false;
	public boolean capacityReached = false;
	
	public StormCrawlerQueue() {
		size.set(WorkerServer.workerStorage.getQueueSize());
		Map<Long, Domain> dbDomains = WorkerServer.workerStorage.getAllDomainObj();
		
		for (Long id : dbDomains.keySet()) {
			System.out.println(id);
			DomainQueue q = new DomainQueue(dbDomains.get(id));
			domainQueues.put(id.intValue(), q);
			domainQueueMap.put(dbDomains.get(id).domain, q);
		}
		
		domainCount.set(dbDomains.size());
	}
	
	/**
	 * Get the domain specific queue based on a given domain
	 * @param domain
	 * @return Queue for given domain
	 */
	public DomainQueue getDomainQueue(String domain) {
		return domainQueueMap.get(domain);
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
		
		// get queue corresponding to URL domain
		DomainQueue q = domainQueueMap.get(info.getDomain());
		
		try {
			// create if domain queue doesn't exist
			if (q == null) {
				Domain d = WorkerServer.workerStorage.addDomainObj(info.getDomain(), domainCount.getAndIncrement());
				q = new DomainQueue(d);
				domainQueues.put((int) d.id, q);
				domainQueueMap.put(info.getDomain(), q);
				
				if (domainQueues.size() >= 1000000) {
					capacityReached = true;
				}
			}
			
			boolean putSuccess = q.put(url);
				
			// only increase size if the url is allowed by robots.txt
			if (putSuccess) {
				size.incrementAndGet();
			}
		} catch (ThreadInterruptedException e) {
		}
	}
	
	public String take() {
		if (pause) {
			return null;
		}
		
		int startIndex = currIndex.get();

		while (true) {
			// if queue is empty, wait
			if (size.get() == 0) {
				return null;
			}
			
			// wrap around if index gets too large
			currIndex.set(currIndex.intValue() % domainCount.intValue());

			try {
				// check if the domain queue is in a crawler delay
				if (domainQueues.containsKey(currIndex.intValue()) && domainQueues.get(currIndex.intValue()).getDelayRemaining() <= 0) {
	//				WorkerServer.crawler.setWorking(true);
					String url = domainQueues.get(currIndex.intValue()).take();
					if (url != null) {
						System.out.println(currIndex.get());
						size.decrementAndGet();
						currIndex.incrementAndGet();
						return url;
					}
				}
			} catch (ThreadInterruptedException e) {
			}
			
			currIndex.incrementAndGet();
			
			// if we've gone to all indexes and not found anything, return null
			if (currIndex.intValue() == startIndex) {
				return null;
			}
		}
	}
}
