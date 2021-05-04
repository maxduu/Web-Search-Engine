package edu.upenn.cis.cis455.crawler;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.LocalCluster;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;

public class Crawler {
	
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
        builder.setSpout(QUEUE_SPOUT, queueSpout, 8);
        builder.setBolt(DOCUMENT_FETCH_BOLT, documentFetchBolt, 12).shuffleGrouping(QUEUE_SPOUT);
        builder.setBolt(LINK_EXTRACTOR_BOLT, linkExtractorBolt, 12).shuffleGrouping(DOCUMENT_FETCH_BOLT);
        
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
    
    public void shutdown() {
    	cluster.killTopology("crawl");
    	cluster.shutdown();
    }
}
