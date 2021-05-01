package edu.upenn.cis.cis455.crawler;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.upenn.cis.cis455.crawler.utils.WorkerRouter;
import edu.upenn.cis.cis455.crawler.worker.WorkerServer;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;

/**
 * Bolt to extract links from a HTML document
 * @author Kevin Chen
 *
 */
public class LinkExtractorBolt implements IRichBolt {
	static Logger log = LogManager.getLogger(LinkExtractorBolt.class);
	
	Fields myFields = new Fields();
	
    String executorId = UUID.randomUUID().toString();
    private OutputCollector collector;
    Crawler crawlerInstance = WorkerServer.crawler;

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
		// TODO Auto-generated method stub
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
			try {
				if (WorkerRouter.sendUrlToWorker(nextUrl, WorkerServer.config.get("workers")).getResponseCode() !=
						HttpURLConnection.HTTP_OK) {
//					WorkerServer.crawler.setWorking(false);
					throw new RuntimeException("Worker add start URL request failed");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
	    }
	    
	    // link extract task finished
//	    WorkerServer.crawler.setWorking(false);
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
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
