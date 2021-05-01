package edu.upenn.cis.cis455.storage;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.json.ReaderBasedJsonParser;

public class WorkerStorage extends RDSStorage implements WorkerStorageInterface {

	@Override
	public List<Integer> batchWriteDocuments(List<Document> documents) throws SQLException {
		List<Integer> documentIds = new ArrayList<Integer>();
		if (documents.size() == 0) {
			return documentIds;
		}
		
		String urlInsertQuery = "INSERT INTO urls (url) VALUES";
		
		for (int i = 0; i < documents.size(); i++) {
			Document doc = documents.get(i);
			if (i == documents.size() - 1) {
				urlInsertQuery += " (" + doc.url + ") RETURNING id;"; 
			} else {
				urlInsertQuery += " (" + doc.url + "),"; 
			}
		}
		
		Connection con = getDBConnection();
		Statement urlInsertStmt = con.createStatement();
        ResultSet urlInsertRs = urlInsertStmt.executeQuery(urlInsertQuery);
        
        while (urlInsertRs.next()) {
        	documentIds.add(urlInsertRs.getInt(1));
        }
        
        String contentInsertQuery = "INSERT INTO crawler_docs (id, content, type) VALUES";
        
		for (int i = 0; i < documents.size(); i++) {
			Document doc = documents.get(i);
			int id = documentIds.get(i);
			if (i == documents.size() - 1) {
				contentInsertQuery += " (" + id + ", " + doc.content + ", " + doc.type + ") RETURNING id;"; 
			} else {
				contentInsertQuery += " (" + id + ", " + doc.content + ", " + doc.type + "),"; 
			}
		}
		
		Statement contentInsertStmt = con.createStatement();
        contentInsertStmt.executeQuery(contentInsertQuery);
        con.close();
		
		return documentIds;
	}

	@Override
	public void batchWriteLinks(List<Link> links) {
		// TODO Auto-generated method stub
		return;
	}
	
	@Override
	public Document getDocumentContent(String url) throws SQLException {
		String query = "SELECT urls.id, urls.url, crawler_docs.content, crawler_docs.type "
				+ "FROM crawler_docs "
				+ "INNER JOIN urls ON urls.id = crawler_docs.id "
				+ "WHERE urls.url=" + url;
		
		Connection con = getDBConnection();
		Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        rs.next();
        int id = rs.getInt(1);
        String docUrl = rs.getString(2);
        String content = rs.getString(3);
        String contentType = rs.getString(4);
        
        con.close();
        return new Document(id, docUrl, content, contentType);
	}

}
