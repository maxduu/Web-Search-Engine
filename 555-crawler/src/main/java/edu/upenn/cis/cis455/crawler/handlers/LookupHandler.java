package edu.upenn.cis.cis455.crawler.handlers;

import spark.Request;
import spark.Route;
import spark.Response;
import spark.HaltException;
import static spark.Spark.halt;

import edu.upenn.cis.cis455.storage.Document;
import edu.upenn.cis.cis455.storage.StorageInterface;

/**
 * Handler for the lookup request
 * @author Kevin Chen
 *
 */
public class LookupHandler implements Route {
    StorageInterface db;

    public LookupHandler(StorageInterface db) {
        this.db = db;
    }

    @Override
    public String handle(Request req, Response resp) throws HaltException {
    	
    	// no url specified in query params
    	String url = req.queryParams("url");
    	if (url == null) {
    		System.err.println("Lookup couldn't find: " + url);
    		halt(404, "<h1>Not Found</h1>");
    	}
    	
    	Document doc = db.getDocumentObjectByUrl(url);
    	
    	// no document exists
    	if (doc == null) {
    		System.err.println("Lookup couldn't find: " + url);
    		halt(404, "<h1>Not Found</h1>");
    	}
    	
    	String content = doc.content;
    	
    	// set the response header to the content type
    	resp.header("Content-Type", doc.type);
    	
    	return content;
    }
}
