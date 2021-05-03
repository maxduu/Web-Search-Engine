package edu.upenn.cis.cis455.crawler;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import edu.upenn.cis.cis455.crawler.utils.WorkerRouter;
import edu.upenn.cis.cis455.crawler.worker.WorkerServer;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.cis455.storage.Link;

/**
 * Bolt to extract links from a HTML document
 * @author Kevin Chen
 *
 */
public class LinkExtractorBolt implements IRichBolt {
	public static final int BATCH_SIZE = 400;
	
	boolean terminated = false;

	ExecutorService executor = Executors.newFixedThreadPool(4);

	static Logger log = LogManager.getLogger(LinkExtractorBolt.class);
	
	Fields myFields = new Fields();
	
    String executorId = UUID.randomUUID().toString();
    private OutputCollector collector;
    Crawler crawlerInstance = WorkerServer.crawler;
    
    List<Link> linkBatch;

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(myFields);		
	}

	@Override
	public void cleanup() {
		if (linkBatch.size() > 0) {
			batchWriteLinks();    	
		}
		executor.shutdown();
		terminated = true;
	}

	@Override
	public void execute(Tuple input) {
		String currentUrl = input.getStringByField("url");
		String content = input.getStringByField("document");
		String type = input.getStringByField("type");

		// ignore non html documents (xml docs)
		if (!type.startsWith("text/html")) {
//			WorkerServer.crawler.setWorking(false);
			System.out.println("IGNORE NON-XML DOCS");
			return;
		}
		
		// use jsoup to find a tags with href links and add to queue
	    Document doc = Jsoup.parse(content, currentUrl);
	    Elements links = doc.select("a");
	    
	    for (Element link : links) {
			String nextUrl = link.absUrl("href");
			
			if (nextUrl.length() == 0) {
				continue;
			}
			
			String normalizedUrl = new URLInfo(nextUrl).toString();
			
			if (normalizedUrl.length() > 2048) {
				continue;
			}
			
			linkBatch.add(new Link(currentUrl, normalizedUrl));
			
			try {
				if (!terminated && WorkerRouter.sendUrlToWorker(nextUrl, WorkerServer.config.get("workers")).getResponseCode() !=
						HttpURLConnection.HTTP_OK) {
//					WorkerServer.crawler.setWorking(false);
					throw new RuntimeException("Worker add start URL request failed");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
	    }
	    
	    if (linkBatch.size() >= BATCH_SIZE) {
	    	batchWriteLinks();
	    }
	    
	    // link extract task finished
//	    WorkerServer.crawler.setWorking(false);
	}
	
	private void batchWriteLinks() {
		
		List<Link> linkBatchCopy = new ArrayList<Link>(linkBatch);
		
		executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					WorkerServer.workerStorage.batchWriteLinks(linkBatchCopy);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		});
    		
	    linkBatch = new ArrayList<Link>();
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.linkBatch = new ArrayList<Link>();
	}

	@Override
	public void setRouter(IStreamRouter router) {
		// TODO Auto-generated method stub
	}

	@Override
	public Fields getSchema() {
		return myFields;
	}

}
