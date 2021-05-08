package edu.upenn.cis.cis455.storage;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;


public class MasterStorage extends RDSStorage implements MasterStorageInterface {
	
	Environment env;
	EntityStore contentSeenStore;
	PrimaryIndex<String, DocumentHash> contentSeenByHash;
	
	/**
	 * Initialize the BerkeleyDB storage
	 * @param directory
	 */
	public MasterStorage(String directory) {
		
		// create the environment
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        
        env = new Environment(new File(directory), envConfig);        
        
        // create the content seen store and indexes
        StoreConfig contentSeenStoreConfig = new StoreConfig();
        contentSeenStoreConfig.setAllowCreate(true);
        contentSeenStoreConfig.setTransactional(true);
        contentSeenStore = new EntityStore(env, "ContentSeen", contentSeenStoreConfig);
        
        contentSeenByHash = contentSeenStore.getPrimaryIndex(String.class, DocumentHash.class);
	}

	// TODO: read from RDS - should only be run one time!!
	@Override
	public int getCorpusSize() throws SQLException {
		Connection con = getDBConnection();
		
		Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM crawler_docs_test");
        rs.next();
        int corpusSize = rs.getInt(1);
        
        con.close();
        return corpusSize;
	}
	
	

	@Override
	public void close() {
		// first need to clear the content seen hash store
        contentSeenStore.truncateClass(DocumentHash.class);
		contentSeenStore.close();
		env.close();
	}

	@Override
	public boolean addDocumentHash(String hashedContent) {
		DocumentHash hashObj = contentSeenByHash.get(hashedContent);
		
		// if the hash has been seen, then we don't need to store again
		if (hashObj != null) {
			return false;
		}
		
		Transaction txn = env.beginTransaction(null, null);
		DocumentHash h = new DocumentHash();
		h.hash = hashedContent;	
		contentSeenByHash.put(h);
		txn.commit();
		
		return true;
	}
}
