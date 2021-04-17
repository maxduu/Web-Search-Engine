package edu.upenn.cis.cis455.storage;

import java.util.Collection;
import java.util.Set;

public interface StorageInterface {

    /**
     * How many documents so far?
     */
    public int getCorpusSize();

    /**
     * Add a new document, getting its ID
     * @param url
     * @param documentContents
     * @param type
     * @param modified - boolean representing if the document was modified and we need to update 
     * the content seen table AND the document entry
     * @return
     */
    public AddDocumentResponse addDocument(String url, String documentContents, String type, boolean modified);

    /**
     * Retrieves a document's contents by URL
     */
    public String getDocument(String url);
    
    /**
     * Returns a boolean saying if the url is in the document store
     * @param url
     * @return
     */
    public boolean urlSeen(String url);
    
    /**
     * Get the document object from BerkeleyDB based on url
     * @param url
     * @return
     */
    public Document getDocumentObjectByUrl(String url);

    /**
     * Shuts down / flushes / closes the storage system
     */
    public void close();

    /**
     * USED FOR TESTING: deletes all entries in the database
     */
	public void clearEntries();

	/**
	 * Get a document object by its id
	 * @param id
	 * @return
	 */
	Document getDocumentObjectById(int id);

}
