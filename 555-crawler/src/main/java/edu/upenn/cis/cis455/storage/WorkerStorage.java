package edu.upenn.cis.cis455.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.json.ReaderBasedJsonParser;

public class WorkerStorage extends RDSStorage implements WorkerStorageInterface {

	@Override
	public List<Integer> batchWriteDocuments(List<Document> documents) throws SQLException {
		System.err.println("BATCH WRITING DOCUMENTS");
		List<Integer> documentIds = new ArrayList<Integer>();
		if (documents.size() == 0) {
			return documentIds;
		}
		
		String urlInsertQuery = "INSERT INTO urls (url) VALUES (?) RETURNING id";
		
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
        
        String contentInsertQuery = "INSERT INTO crawler_docs (id, content, type) VALUES (?, ?, ?)";
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
	public void batchWriteLinks(List<Link> links) {
		// TODO Auto-generated method stub
		return;
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
        
        System.out.println("IN GET DOC");

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

}
