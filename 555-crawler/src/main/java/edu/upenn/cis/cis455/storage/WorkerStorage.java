package edu.upenn.cis.cis455.storage;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;

import com.fasterxml.jackson.core.json.ReaderBasedJsonParser;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityIndex;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;

public class WorkerStorage extends RDSStorage implements WorkerStorageInterface {

	Environment env;

	EntityStore urlSeenStore;
	PrimaryIndex<String, URLSeenTime> dateByUrl;
	
	EntityStore domainStore;
	PrimaryIndex<Long, Domain> domainById;
	
	EntityStore queueUrlStore;
	PrimaryIndex<String, QueueURL> queueUrlByUrl;
	SecondaryIndex<Long, String, QueueURL> queueUrlByDomainId;
	
	public WorkerStorage(String directory) {
		// create the environment
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        
        env = new Environment(new File(directory), envConfig);
        
        // create store and indexes
        StoreConfig urlSeenStoreConfig = new StoreConfig();
        urlSeenStoreConfig.setAllowCreate(true);
        urlSeenStoreConfig.setTransactional(true);
        urlSeenStore = new EntityStore(env, "UrlSeenStore", urlSeenStoreConfig);
        
        dateByUrl = urlSeenStore.getPrimaryIndex(String.class, URLSeenTime.class);
        
        // create store and indexes
        StoreConfig domainStoreConfig = new StoreConfig();
        domainStoreConfig.setAllowCreate(true);
        domainStoreConfig.setTransactional(true);
        domainStore = new EntityStore(env, "DomainStore", domainStoreConfig);
        
        domainById = domainStore.getPrimaryIndex(Long.class, Domain.class);
        
        // create store and indexes
        StoreConfig queueUrlStoreConfig = new StoreConfig();
        queueUrlStoreConfig.setAllowCreate(true);
        queueUrlStoreConfig.setTransactional(true);
        queueUrlStore = new EntityStore(env, "QueueURLStore", queueUrlStoreConfig);
        
        queueUrlByUrl = queueUrlStore.getPrimaryIndex(String.class, QueueURL.class);
        queueUrlByDomainId = queueUrlStore.getSecondaryIndex(queueUrlByUrl, Long.class, "domainId");
	}

	@Override
	public List<Integer> batchWriteDocuments(List<Document> documents) throws SQLException {
		System.err.println("BATCH WRITING DOCUMENTS");
		List<Integer> documentIds = new ArrayList<Integer>();
		if (documents.size() == 0) {
			return documentIds;
		}
		
		String urlInsertQuery = "INSERT INTO urls (url) VALUES (?) "
				+ "ON CONFLICT (url) DO UPDATE SET url=excluded.url "
				+ "RETURNING id";
		
		Connection con = getDBConnection();
		con.setAutoCommit(false);  
		PreparedStatement urlInsertStmt = con.prepareStatement(urlInsertQuery, Statement.RETURN_GENERATED_KEYS);
		
		for (Document doc : documents) {
			urlInsertStmt.setString(1, doc.url);
			urlInsertStmt.addBatch();
		}
		
		urlInsertStmt.executeBatch();
		ResultSet urlInsertRs = urlInsertStmt.getGeneratedKeys();
        
        while (urlInsertRs.next()) {
        	documentIds.add(urlInsertRs.getInt(1));
        }
        
        String contentInsertQuery = "INSERT INTO crawler_docs (id, content, type) VALUES (?, ?, ?) "
        		+ "ON CONFLICT (id) DO UPDATE SET content=excluded.content, type=excluded.type";
		PreparedStatement contentInsertStmt = con.prepareStatement(contentInsertQuery);
        
		for (int i = 0; i < documents.size(); i++) {
			Document doc = documents.get(i);
			int id = documentIds.get(i);
			contentInsertStmt.setInt(1, id);
			contentInsertStmt.setString(2, doc.content);
			contentInsertStmt.setString(3, doc.type);
			contentInsertStmt.addBatch();
		}
		
        contentInsertStmt.executeBatch();
        con.commit();

        con.close();		
		return documentIds;
	}

	@Override
	public void batchWriteLinks(List<Link> links) throws SQLException {
		System.err.println("BATCH WRITING LINKS");
		if (links.size() == 0) {
			return;
		}
		
		String query = "INSERT INTO links_url (source, dest) VALUES (?, ?) "
				+ "ON CONFLICT (source, dest) DO NOTHING";
		
		Connection con = getDBConnection();
		con.setAutoCommit(false);  
		PreparedStatement stmt = con.prepareStatement(query);
		
		for (Link link : links) {
			stmt.setString(1, link.sourceUrl);
			stmt.setString(2, link.destUrl);
			stmt.addBatch();
		}
		
		stmt.executeBatch();
        con.commit();
        con.close();	
	}
	
	@Override
	public Document getDocumentContent(String url) throws SQLException {
		String query = "SELECT urls.id, urls.url, crawler_docs.content, crawler_docs.type "
				+ "FROM crawler_docs "
				+ "INNER JOIN urls ON urls.id = crawler_docs.id "
				+ "WHERE urls.url= ?";
		
		Connection con = getDBConnection();
		PreparedStatement stmt = con.prepareStatement(query);
		stmt.setString(1, url);
        ResultSet rs = stmt.executeQuery();

        if (!rs.next()) {
        	return null;
        }
        int id = rs.getInt(1);
        String docUrl = rs.getString(2);
        String content = rs.getString(3);
        String contentType = rs.getString(4);
        
        con.close();
        return new Document(id, docUrl, content, contentType);
	}

	@Override
	public void addUrlSeen(String url, Date lastCrawled) {
		Transaction txn = env.beginTransaction(null, null);

		URLSeenTime obj = new URLSeenTime();
		obj.url = url;
		obj.lastCrawled = lastCrawled;
		
		dateByUrl.put(obj);
		
		txn.commit();
	}

	@Override
	public URLSeenTime getUrlSeen(String url) {
		return dateByUrl.get(url);
	}

	@Override
	public void close() {
		urlSeenStore.close();
		domainStore.close();
		queueUrlStore.close();
		env.close();
	}

	@Override
	public Map<Long, Domain> getAllDomainObj() {
		return domainById.sortedMap();
	}
	
	private String getRobotsTxt(String domain) {
		// construct url and connection to get the content
		URL urlObj;
		try {
			urlObj = new URL(domain + "/robots.txt");
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return "";
		}
		
    	HttpURLConnection urlConnection;

    	try {
			if (domain.startsWith("https")) {
				urlConnection = (HttpsURLConnection) urlObj.openConnection();
			} else {
				urlConnection = (HttpURLConnection) urlObj.openConnection();
			}
    	} catch(IOException e) {
    		e.printStackTrace();
    		return "";
    	}
    	
		urlConnection.setRequestProperty("User-Agent", "cis455crawler");
		
		try {
			BufferedInputStream in = new BufferedInputStream(urlConnection.getInputStream());
		    return new String(in.readAllBytes());
		} catch (IOException e) {
			// case when no robots.txt exists
			return "";
		}
	}

	@Override
	public Domain addDomainObj(String domain, int id) {		
		String robotsTxt = getRobotsTxt(domain);
		
		Transaction txn = env.beginTransaction(null, null);
		
		Domain d = new Domain();
		d.domain = domain;
		d.id = id;
		d.robotsTxtContent = robotsTxt;
		
		domainById.put(d);
		
		txn.commit();
		return d;
	}

	@Override
	public boolean addQueueUrl(String url, long domainId) {
		if (queueUrlByUrl.contains(url)) {
			return false;
		}
		
		Transaction txn = env.beginTransaction(null, null);
		
		QueueURL qUrl = new QueueURL();
		qUrl.domainId = domainId;
		qUrl.url = url;
		
		queueUrlByUrl.put(qUrl);
		
		txn.commit();
		return true;
	}

	@Override
	public String takeQueueUrl(long domainId) {
		QueueURL url = queueUrlByDomainId.get(domainId);
		if (url == null) {
			return null;
		}
		
		Transaction txn = env.beginTransaction(null, null);
		queueUrlByUrl.delete(url.url);
		txn.commit();

		return url.url;
	}

	@Override
	public long getQueueSize() {
		return queueUrlByUrl.count();
	}

}
