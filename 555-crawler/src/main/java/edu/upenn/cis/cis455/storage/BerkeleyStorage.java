package edu.upenn.cis.cis455.storage;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityIndex;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;

import edu.upenn.cis.cis455.crawler.utils.URLInfo;

public class BerkeleyStorage implements StorageInterface {
	
	Environment env;
	
	EntityStore documentStore;
	EntityStore contentSeenStore;

	PrimaryIndex<Integer, Document> documentById;
	SecondaryIndex<String, Integer, Document> documentByUrl;

	PrimaryIndex<String, DocumentHash> contentSeenByHash;
	
	/**
	 * Initialize the BerkeleyDB storage
	 * @param directory
	 */
	public BerkeleyStorage(String directory) {
		
		// create the environment
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        
        env = new Environment(new File(directory), envConfig);        
        
        // create the document store and indexes
        StoreConfig documentStoreConfig = new StoreConfig();
        documentStoreConfig.setAllowCreate(true);
        documentStoreConfig.setTransactional(true);
        documentStore = new EntityStore(env, "DocumentStore", documentStoreConfig);
        
        documentById = documentStore.getPrimaryIndex(Integer.class, Document.class);
        documentByUrl = documentStore.getSecondaryIndex(documentById, String.class, "url");
        
        // create the content seen store and indexes
        StoreConfig contentSeenStoreConfig = new StoreConfig();
        contentSeenStoreConfig.setAllowCreate(true);
        contentSeenStoreConfig.setTransactional(true);
        contentSeenStore = new EntityStore(env, "ContentSeen", contentSeenStoreConfig);
        
        contentSeenByHash = contentSeenStore.getPrimaryIndex(String.class, DocumentHash.class);
	}

	@Override
	public int getCorpusSize() {
		return (int) documentById.count();
	}

	@Override
	public AddDocumentResponse addDocument(String url, String documentContents, String type, boolean modified) {
		// create a hash of the contents
		MessageDigest digest;
		String hashedContent = "";
		String normalizedUrl = new URLInfo(url).toString();

		try {
			digest = MessageDigest.getInstance("MD5");
			byte[] encodedhash = digest.digest(documentContents.getBytes(StandardCharsets.UTF_8));
			hashedContent = new String(encodedhash, StandardCharsets.UTF_8);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		
		DocumentHash hashObj = contentSeenByHash.get(hashedContent);
		
		// if the hash has been seen, then we don't need to store again
		if (hashObj != null) {
			return new AddDocumentResponse(hashObj.documentId, true);
		}
		
		Document oldDoc = getDocumentObjectByUrl(normalizedUrl);
		
		int docId = -1;
		if (oldDoc != null) {
			docId = oldDoc.id;
		}
				
		// start the database transaction
		Transaction txn = env.beginTransaction(null, null);
		
		// if the document was modified or we are getting a new document, we store/update
		if (modified) {
			Document d = new Document();
						
			// if a document with the same url exists, we are updating, otherwise we will create a new entry
			if (oldDoc != null) {
				d.id = oldDoc.id;
			}

			d.url = normalizedUrl;
			d.content = documentContents;
			d.type = type;
			d.lastCrawled = new Date();
			documentById.put(d);
			
			docId = d.id;
		}
		
		// store the hash in all cases
		DocumentHash h = new DocumentHash();
		h.hash = hashedContent;	
		h.documentId = docId;
		contentSeenByHash.put(h);
				
		txn.commit();
		
		return new AddDocumentResponse(docId, false);
	}
	
	@Override
	public Document getDocumentObjectByUrl(String url) {
		String normalizedUrl = new URLInfo(url).toString();
		return documentByUrl.get(normalizedUrl);
	}
	
	@Override
	public Document getDocumentObjectById(int id) {
		return documentById.get(id);
	}
	
	@Override
	public boolean urlSeen(String url) {
		String normalizedUrl = new URLInfo(url).toString();
		return documentByUrl.contains(normalizedUrl);
	}

	@Override
	public String getDocument(String url) {
		if (getDocumentObjectByUrl(url) == null) {
			return null;
		}
		
		return getDocumentObjectByUrl(url).content;
	}
	
	@Override
	public void clearEntries() {
        contentSeenStore.truncateClass(DocumentHash.class);
        documentStore.truncateClass(Document.class);
	}

	@Override
	public void close() {
		
		// first need to clear the content seen hash store
        contentSeenStore.truncateClass(DocumentHash.class);
        
		documentStore.close();
		contentSeenStore.close();
		env.close();
	}
}
