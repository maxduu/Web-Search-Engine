package edu.upenn.cis.cis455.crawler;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.HttpsURLConnection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.cis455.crawler.utils.HttpDateUtils;
import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import edu.upenn.cis.cis455.crawler.utils.WorkerRouter;
import edu.upenn.cis.cis455.crawler.worker.WorkerServer;
import edu.upenn.cis.cis455.storage.Document;
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
	
	ExecutorService executor = Executors.newFixedThreadPool(4);
	Fields schema = new Fields("url", "document", "type");
    String executorId = UUID.randomUUID().toString();
    private OutputCollector collector;
    Crawler crawlerInstance = WorkerServer.crawler;
    
    Map<String, Date> urlSeen;
    
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
			batchWriteDocuments(false);    	
		}
		executor.shutdown();
	}
	
	private void checkBatchWrite() {
//	    System.out.println("DOCUMENT BATCH SIZE: " + documentBatch.size() + ", " + documentBatch);
//	    System.out.println("QUEUE SIZE: " + crawlerInstance.queue.size);
	    if (documentBatch.size() >= BATCH_SIZE || crawlerInstance.queue.size == 0) {
	    	batchWriteDocuments(true);
	    }
	}

	@Override
	public void execute(Tuple input) {
        String url = input.getStringByField("url");
        log.debug(getExecutorId() + " received " + url);
//        System.err.println(getExecutorId() + " received " + url);
        
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
//					WorkerServer.crawler.setWorking(false);
					System.err.println(url + " Content type mismatch: " + urlConnection.getHeaderField("Content-Type"));
					checkBatchWrite();
					return;
				}
			} else {
//				WorkerServer.crawler.setWorking(false);
				System.err.println(url + " Content type null");
				checkBatchWrite();
				return;
			}
			
			// check if content length is too long
			if (urlConnection.getHeaderField("Content-Length") != null) {
				int contentLength = Integer.parseInt(urlConnection.getHeaderField("Content-Length"));
				if (contentLength > 1000000 * crawlerInstance.maxDocSize) {
//					WorkerServer.crawler.setWorking(false);
					checkBatchWrite();
					return;
				}
			} 

//			else if (urlConnection.getHeaderField("Transfer-Encoding") == null || 
//					!urlConnection.getHeaderField("Transfer-Encoding").equals("chunked")) {
//				System.err.println("Missing Content-Length");
//				System.err.println(urlConnection.getHeaderFields());
////				WorkerServer.crawler.setWorking(false);
//				return;
//			}
			
			// get final url after redirects
	    	currentUrl = urlConnection.getURL().toString();
	    	URLInfo currentUrlInfo = new URLInfo(currentUrl);
	    	
	    	String currentUrlNormalized = currentUrlInfo.toString();
	    	
	    	// if the redirect url is on a different domain, add back into queue for the 
	    	// correct domain
	    	if (!urlInfo.getDomain().equals(currentUrlInfo.getDomain())) {
	    		WorkerRouter.sendUrlToWorker(currentUrlNormalized, WorkerServer.config.get("workers"));
//	    		WorkerServer.crawler.setWorking(false);
				checkBatchWrite();
	    		return;
	    	} else if (crawlerInstance.queue.getDomainQueue(currentUrlInfo.getDomain()) != null) {
	    		DomainQueue dq = crawlerInstance.queue.getDomainQueue(currentUrlInfo.getDomain());
	    		
	    		// if domain is the same domain, make sure the redirect url is allowed
	    		if (dq.checkDisallowed(currentUrl)) {
//	    			WorkerServer.crawler.setWorking(false);
					checkBatchWrite();
	    			return;
	    		}
	    	}
			
	    	// check if the url has been stored before and see if it has been modified since
			if (urlSeen.containsKey(currentUrlNormalized)) {
				Date lastCrawled = urlSeen.get(currentUrlNormalized);
				
				if (crawlerInstance.startDate.before(lastCrawled)) {
					checkBatchWrite();
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
							} else {
	//							WorkerServer.crawler.setWorking(false);
							}
						}
						checkBatchWrite();
						return;
					}
				}
			}
			
			urlSeen.put(currentUrlNormalized, new Date());
	    				
			// open new url connection to fetch the content
			if (currentUrlInfo.isSecure()) {
				urlConnection = (HttpsURLConnection) urlObj.openConnection();
			} else {
				urlConnection = (HttpURLConnection) urlObj.openConnection();
			}
			
			urlConnection.setRequestProperty("User-Agent", "cis455crawler");
			urlConnection.setConnectTimeout(5000);
			urlConnection.setReadTimeout(5000);
			
			// download the content
			in = new BufferedInputStream(urlConnection.getInputStream());
//	    	log.info(url + ": downloading");
		    String content = new String(in.readAllBytes());
		    		    		    
		    // add content to the database - this will check if the document contents 
		    // have been hashed and index accordingly
			int resCode = WorkerRouter.sendDocumentHashToMaster(WorkerServer.masterServer, content)
					.getResponseCode();
		    
			if (resCode != HttpURLConnection.HTTP_OK) {
				System.err.println(url + " Content seen");
				checkBatchWrite();
		    	return;
			}

		    documentBatch.add(new Document(currentUrlNormalized, content, urlConnection.getHeaderField("Content-Type")));		    
        } catch (IOException e) {
//        	WorkerServer.crawler.setWorking(false);
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        checkBatchWrite();
	}
	
	private void batchWriteDocuments(boolean send) {
		
		List<Document> documentBatchCopy = new ArrayList<Document>(documentBatch);
		
		executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					List<Integer> documentIds = WorkerServer.workerStorage.batchWriteDocuments(documentBatchCopy);
			    	if (send) {
				    	for (int i = 0; i < documentIds.size(); i++) {
				    		Document doc = documentBatchCopy.get(i);
							collector.emit(new Values<Object>(doc.getUrl(), doc.getContent(), doc.getType()));
				    	}
			    	}
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
        this.urlSeen = new HashMap<String, Date>();
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
