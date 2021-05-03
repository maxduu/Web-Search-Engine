package edu.upenn.cis.cis455.crawler;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.LocalCluster;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;

public class Crawler implements CrawlMaster {
	
	private static final String QUEUE_SPOUT = "QUEUE_SPOUT";
    private static final String DOCUMENT_FETCH_BOLT = "DOCUMENT_FETCH_BOLT";
    private static final String LINK_EXTRACTOR_BOLT = "LINK_EXTRACTOR_BOLT";
    
    public Date startDate;
	
    public StormCrawlerQueue queue = new StormCrawlerQueue();
    public int maxDocSize;
    public int count;
    
    AtomicInteger tasks = new AtomicInteger();
    
    LocalCluster cluster;

    public Crawler(int size, int count) {
    	this.maxDocSize = size;
    	this.count = count;
    }

    /**
     * Main thread
     */
    public void start() {
    	// add start url to the queue
        Config config = new Config();

        // build the crawler storm topology
        QueueSpout queueSpout = new QueueSpout();
        DocumentFetchBolt documentFetchBolt = new DocumentFetchBolt();
        LinkExtractorBolt linkExtractorBolt = new LinkExtractorBolt();
        
        // build the topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(QUEUE_SPOUT, queueSpout, 6);
        builder.setBolt(DOCUMENT_FETCH_BOLT, documentFetchBolt, 8).shuffleGrouping(QUEUE_SPOUT);
        builder.setBolt(LINK_EXTRACTOR_BOLT, linkExtractorBolt, 8).shuffleGrouping(DOCUMENT_FETCH_BOLT);
        
        cluster = new LocalCluster();
        Topology topo = builder.createTopology();

        ObjectMapper mapper = new ObjectMapper();
		try {
			String str = mapper.writeValueAsString(topo);

			System.out.println("The StormLite topology is:\n" + str);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
		// submit topology to the cluster
        cluster.submitTopology("crawl", config, 
        		builder.createTopology());
        
        startDate = new Date();
    }

    @Override
    public boolean isWorking() {
    	// we are done when the queue is empty or we've gotten the max number of docs and all worker tasks 
    	// are finished
//        return this.tasks.get() != 0;
    	return false;
    }

    /**
     * Workers should notify when they are processing an URL
     */
//    @Override
//    public void setWorking(boolean working) {
//    	if (working) {
//    		this.tasks.incrementAndGet(); // one for the link extractor
//    	} else {
//    		this.tasks.decrementAndGet(); // called when link extractor finishes
//    	}
//    	System.out.println(tasks);
//    }

    /**
     * Workers should call this when they exit, so the master knows when it can shut
     * down
     */
    @Override
    public synchronized void notifyThreadExited() {
    }
    
    public void shutdown() {
    	cluster.killTopology("crawl");
    	cluster.shutdown();
    }

    /**
     * Main program: init database, start crawler, wait for it to notify that it is
     * done, then close.
     */
//    public static void main(String args[]) {
//        if (args.length < 3 || args.length > 5) {
//            System.out.println("Usage: Crawler {start URL} {database environment path} {max doc size in MB} {number of files to index}");
//            System.exit(1);
//        }
//
//        System.out.println("Crawler starting");
//        String startUrl = args[0];
//        String envPath = args[1];
//        Integer size = Integer.valueOf(args[2]);
//        Integer count = args.length == 4 ? Integer.valueOf(args[3]) : 100;
//        
//        if (!Files.exists(Paths.get(envPath))) {
//            try {
//                Files.createDirectory(Paths.get(envPath));
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//        StorageInterface db;
//        
//		db = StorageFactory.getDatabaseInstance(envPath);
//		
//        Crawler crawler = new Crawler(startUrl, db, size, count);
//
//        System.out.println("Starting crawl of " + count + " documents, starting at " + startUrl);
//        crawler.start();
//
//        while (!crawler.isDone())
//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//
//        crawler.shutdown();
//        
//        System.out.println("Done crawling!");
//    }

}
