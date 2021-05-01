package edu.upenn.cis.cis455.storage;

import java.sql.SQLException;
import java.util.List;
import java.util.Queue;

public interface WorkerStorageInterface {

	public List<Integer> batchWriteDocuments(List<Document> documents) throws SQLException;
	
	public void batchWriteLinks(List<Link> links);
	
	public Document getDocumentContent(String url) throws SQLException;

}
