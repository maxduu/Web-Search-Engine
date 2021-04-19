package edu.upenn.cis.cis455.crawler;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import javax.net.ssl.HttpsURLConnection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.cis455.crawler.utils.HttpDateUtils;
import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import edu.upenn.cis.cis455.storage.AddDocumentResponse;
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
	
	Fields schema = new Fields("id", "url", "document", "type");
    String executorId = UUID.randomUUID().toString();
    private OutputCollector collector;
    Crawler crawlerInstance = Crawler.getSingleton();

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
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
        String url = input.getStringByField("url");
        log.debug(getExecutorId() + " received " + url);
        
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
			
			String contentType = "";
			
			// check if content type is html or xml
			if (urlConnection.getHeaderField("Content-Type") != null) {
				contentType = urlConnection.getHeaderField("Content-Type");
				if (!contentType.startsWith("text/html") && !contentType.startsWith("text/xml") && 
						!contentType.startsWith("application/xml") && !contentType.contains("+xml")) {
					Crawler.getSingleton().setWorking(false);
					return;
				}
			} else {
				Crawler.getSingleton().setWorking(false);
				return;
			}
			
			// check if content length is too long
			if (urlConnection.getHeaderField("Content-Length") != null) {
				int contentLength = Integer.parseInt(urlConnection.getHeaderField("Content-Length"));
				if (contentLength > 1000000 * crawlerInstance.maxDocSize) {
					Crawler.getSingleton().setWorking(false);
					return;
				}
			} else {
				Crawler.getSingleton().setWorking(false);
				return;
			}
			
			// get final url after redirects
	    	currentUrl = urlConnection.getURL().toString();
	    	URLInfo currentUrlInfo = new URLInfo(currentUrl);
	    	
	    	// if the redirect url is on a different domain, add back into queue for the 
	    	// correct domain
	    	if (!urlInfo.getDomain().equals(currentUrlInfo.getDomain())) {
	    		crawlerInstance.queue.put(currentUrl);
				Crawler.getSingleton().setWorking(false);
	    		return;
	    	} else if (crawlerInstance.queue.getDomainQueue(currentUrlInfo.getDomain()) != null) {
	    		DomainQueue dq = crawlerInstance.queue.getDomainQueue(currentUrlInfo.getDomain());
	    		
	    		// if domain is the same domain, make sure the redirect url is allowed
	    		if (dq.checkDisallowed(currentUrl)) {
	    			Crawler.getSingleton().setWorking(false);
	    			return;
	    		}
	    	}
			
	    	// check if the url has been stored before and see if it has been modified since
			if (crawlerInstance.db.urlSeen(currentUrl)) {
				edu.upenn.cis.cis455.storage.Document doc = crawlerInstance.db.getDocumentObjectByUrl(currentUrl);
				if (urlConnection.getHeaderField("Last-Modified") != null) {
					String lastModifiedString = urlConnection.getHeaderField("Last-Modified");
					Date lastModified = HttpDateUtils.parseHttpString(lastModifiedString);
					if (lastModified.before(doc.lastCrawled)) {
						
						// document wasn't modified, but we still need to add the hash
						AddDocumentResponse res = crawlerInstance.db.addDocument(currentUrl, doc.content, contentType, false);
						log.info(url + ": not modified");

						// we can use the db copy if we haven't hashed the doc yet
						if (!res.contentSeen) {
							collector.emit(new Values<Object>(res.documentId, currentUrl, doc.content, 
									urlConnection.getHeaderField("Content-Type")));
						} else {
							Crawler.getSingleton().setWorking(false);
						}
						return;
					}
				}
			}
			
			// open new url connection to fetch the content
			if (currentUrlInfo.isSecure()) {
				urlConnection = (HttpsURLConnection) urlObj.openConnection();
			} else {
				urlConnection = (HttpURLConnection) urlObj.openConnection();
			}
			
			urlConnection.setRequestProperty("User-Agent", "cis455crawler");
			
			// download the content
			in = new BufferedInputStream(urlConnection.getInputStream());
	    	log.info(url + ": downloading");
		    String content = new String(in.readAllBytes());
		    		    
		    // add content to the database - this will check if the document contents 
		    // have been hashed and index accordingly
		    AddDocumentResponse res = crawlerInstance.db.addDocument(currentUrl, content, contentType, true);
		    		    
		    // if the contents haven't been hashed and we indexed the doc, we increment 
		    // the crawler's counts and emit
		    if (!res.contentSeen) {
		    	crawlerInstance.incCount();
				collector.emit(new Values<Object>(res.documentId, currentUrl, content, 
						urlConnection.getHeaderField("Content-Type")));
		    } else {
				Crawler.getSingleton().setWorking(false);
		    }
        } catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
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
