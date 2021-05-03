package edu.upenn.cis.cis455.storage;

import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Queue;

public interface WorkerStorageInterface {

	public List<Integer> batchWriteDocuments(List<Document> documents) throws SQLException;
	
	public void batchWriteLinks(List<Link> links) throws SQLException;
	
	public Document getDocumentContent(String url) throws SQLException;
	
	public void addUrlSeen(String url, Date lastCrawled);
	
	public URLSeenTime getUrlSeen(String url);
	
	public void close();

}
