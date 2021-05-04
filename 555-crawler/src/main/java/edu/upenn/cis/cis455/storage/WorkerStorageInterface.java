package edu.upenn.cis.cis455.storage;

import java.sql.SQLException;
import java.util.Date;
import java.util.List;

public interface WorkerStorageInterface {

	public void batchWriteDocuments(List<Document> documents) throws SQLException;
	
	public void batchWriteLinks(List<Link> links) throws SQLException;
	
	public Document getDocumentContent(String url) throws SQLException;
	
	public void addUrlSeen(String url, Date lastCrawled);
	
	public URLSeenTime getUrlSeen(String url);
	
	public void close();
	
	public boolean addQueueUrl(String url, Date dateAdded);
	
	public String takeQueueUrl();
	
	public long getQueueSize();

}
