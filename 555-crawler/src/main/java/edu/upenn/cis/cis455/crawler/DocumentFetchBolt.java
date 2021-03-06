package edu.upenn.cis.cis455.crawler;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.HttpsURLConnection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.cis455.crawler.utils.HttpDateUtils;
import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import edu.upenn.cis.cis455.crawler.utils.WorkerRouter;
import edu.upenn.cis.cis455.crawler.worker.WorkerServer;
import edu.upenn.cis.cis455.storage.Document;
import edu.upenn.cis.cis455.storage.URLSeenTime;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;

/**
 * Bolt to fetch the document either via HTTP request or local db copy
 * @author Kevin Chen
 *
 */
public class DocumentFetchBolt implements IRichBolt {
	static Logger log = LogManager.getLogger(DocumentFetchBolt.class);
	
	public static final int BATCH_SIZE = 10;
	
	boolean terminated = false;
	
	ExecutorService executor = Executors.newFixedThreadPool(4);
	Fields schema = new Fields("url", "document", "type");
    String executorId = UUID.randomUUID().toString();
    private OutputCollector collector;
    Crawler crawlerInstance = WorkerServer.crawler;
    
    List<Document> documentBatch;

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(schema);		
	}

	@Override
	public void cleanup() {
		if (documentBatch.size() > 0) {
			batchWriteDocuments();    	
		}
		executor.shutdown();
		terminated = true;
	}

	@Override
	public void execute(Tuple input) {
        String url = input.getStringByField("url");
        log.debug(getExecutorId() + " received " + url);
        //System.err.println(getExecutorId() + " received " + url);
        
        try {
	        URL urlObj = new URL(url);
			URLInfo urlInfo = new URLInfo(url);
	
			BufferedInputStream in;
	    	String currentUrl;
	    	HttpURLConnection urlConnection;
	    	
	    	// open url connection based on protocol
			if (urlInfo.isSecure()) {
				urlConnection = (HttpsURLConnection) urlObj.openConnection();
			} else {
				urlConnection = (HttpURLConnection) urlObj.openConnection();
			}
			
			// set request method and user agent
			urlConnection.setRequestMethod("HEAD");
			urlConnection.setRequestProperty("User-Agent", "cis455crawler");
			urlConnection.setConnectTimeout(5000);
			urlConnection.setReadTimeout(5000);
			
			String contentType = "";
			
			// check if content type is html or xml
			if (urlConnection.getHeaderField("Content-Type") != null) {
				contentType = urlConnection.getHeaderField("Content-Type");
				if (!contentType.startsWith("text/html") && !contentType.startsWith("text/xml") && 
						!contentType.startsWith("application/xml") && !contentType.contains("+xml")) {
					//System.err.println(url + " Content type mismatch: " + urlConnection.getHeaderField("Content-Type"));
					return;
				}
			} else {
				//System.err.println(url + " Content type null");
				return;
			}
			
			// check if content length is too long
			if (urlConnection.getHeaderField("Content-Length") != null) {
				int contentLength = Integer.parseInt(urlConnection.getHeaderField("Content-Length"));
				if (contentLength > 1000000 * crawlerInstance.maxDocSize) {
					return;
				}
			}
			
			// get final url after redirects
	    	currentUrl = urlConnection.getURL().toString();
	    	URLInfo currentUrlInfo = new URLInfo(currentUrl);
	    	
	    	String currentUrlNormalized = currentUrlInfo.toString();
	    	
	    	// if the redirect url is on a different domain, add back into queue for the 
	    	// correct domain
	    	if (!urlInfo.getDomain().equals(currentUrlInfo.getDomain())) {
	    		WorkerRouter.sendUrlToWorker(currentUrlNormalized, WorkerServer.config.get("workers"));
	    		return;
	    	} else if (crawlerInstance.queue.getDomainManager(currentUrlInfo.getDomain()) != null) {
	    		DomainManager dq = crawlerInstance.queue.getDomainManager(currentUrlInfo.getDomain());
	    		
	    		// if domain is the same domain, make sure the redirect url is allowed
	    		if (dq.checkDisallowed(currentUrl)) {
	    			return;
	    		}
	    	}
			
	    	// check if the url has been stored before and see if it has been modified since
	    	URLSeenTime urlSeen = WorkerServer.workerStorage.getUrlSeen(currentUrlNormalized);
	    	
			if (urlSeen != null) {
				Date lastCrawled = urlSeen.lastCrawled;
				
				if (crawlerInstance.startDate.before(lastCrawled)) {
					return;
				}
				
				if (urlConnection.getHeaderField("Last-Modified") != null) {
					String lastModifiedString = urlConnection.getHeaderField("Last-Modified");
					Date lastModified = HttpDateUtils.parseHttpString(lastModifiedString);
					if (lastModified.before(lastCrawled)) { // add and more than one hour difference

						// document wasn't modified, but we still need to add the hash
						Document oldDoc = WorkerServer.workerStorage.getDocumentContent(currentUrlNormalized);
						
						if (oldDoc != null) {
							int resCode = WorkerRouter.sendDocumentHashToMaster(WorkerServer.masterServer, oldDoc.getContent())
									.getResponseCode();
	
							// we can use the db copy if we haven't hashed the doc yet
							if (resCode == 200) {
								collector.emit(new Values<Object>(currentUrlNormalized, oldDoc.getContent(), 
										urlConnection.getHeaderField("Content-Type")));
							}
						}
						return;
					}
				}
			}
			
			WorkerServer.workerStorage.addUrlSeen(currentUrlNormalized, new Date());
	    				
			// open new url connection to fetch the content
			if (currentUrlInfo.isSecure()) {
				urlConnection = (HttpsURLConnection) urlObj.openConnection();
			} else {
				urlConnection = (HttpURLConnection) urlObj.openConnection();
			}
			
			urlConnection.setRequestProperty("User-Agent", "cis455crawler");
			urlConnection.setConnectTimeout(5000);
			urlConnection.setReadTimeout(5000);
			
			if (urlConnection.getResponseCode() >= 400) {
				//System.err.println(currentUrlNormalized + " bad response code " + urlConnection.getResponseCode());
				return;
			}
			
			// download the content
			in = new BufferedInputStream(urlConnection.getInputStream());
	    	//log.info(url + ": downloading");
		    String content = new String(in.readAllBytes());
		    		    		    
		    // add content to the database - this will check if the document contents 
		    // have been hashed and index accordingly
			int resCode = WorkerRouter.sendDocumentHashToMaster(WorkerServer.masterServer, content)
					.getResponseCode();
		    
			if (resCode != HttpURLConnection.HTTP_OK) {
				//System.err.println(currentUrlNormalized + " Content seen " + resCode);
		    	return;
			}

		    documentBatch.add(new Document(currentUrlNormalized, content.replaceAll("\u0000", ""), 
		    		urlConnection.getHeaderField("Content-Type")));	

			collector.emit(new Values<Object>(currentUrlNormalized, content, urlConnection.getHeaderField("Content-Type")));
			
		    if (documentBatch.size() >= BATCH_SIZE) {
		    	batchWriteDocuments();
		    }
        } catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void batchWriteDocuments() {		
		List<Document> documentBatchCopy = new ArrayList<Document>(documentBatch);
		
		executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					WorkerServer.workerStorage.batchWriteDocuments(documentBatchCopy);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		});
    		
	    documentBatch = new ArrayList<Document>();
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.documentBatch = new ArrayList<Document>();
	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);		
	}

	@Override
	public Fields getSchema() {
		return schema;
	}

}
