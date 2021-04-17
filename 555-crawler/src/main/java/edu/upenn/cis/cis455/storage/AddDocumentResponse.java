package edu.upenn.cis.cis455.storage;

/**
 * Class to get if document id and if the content was previously hashed after trying to add a 
 * document to the database
 * @author Kevin Chen
 *
 */
public class AddDocumentResponse {
	public int documentId;
	public boolean contentSeen;
	
	public AddDocumentResponse(int id, boolean contentSeen) {
		this.documentId = id;
		this.contentSeen = contentSeen;
	}
}
