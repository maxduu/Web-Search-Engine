package edu.upenn.cis.cis455;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.cis455.storage.StorageInterface;
import junit.framework.TestCase;

public class TestStorage extends TestCase {
	
	private static String testPath = "./test";
	
    @Before
    public void setUp() {
        if (!Files.exists(Paths.get(testPath))) {
            try {
                Files.createDirectory(Paths.get(testPath));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }  
    }
    
    /**
     * Makes sure that we don't index the same document with same contents
     */
    @Test
    public void testDontStoreSameDocument() {
		StorageInterface database = StorageFactory.getDatabaseInstance(testPath);
    	
    	database.addDocument("http://yahoo.com", "HELLO", "text/html", true);  
    	database.addDocument("http://google.com", "HELLO", "text/html", true);  

        assertEquals(1, database.getCorpusSize());
        
		database.clearEntries();
		database.close();
    }
    
    /**
     * Makes sure we are able to lookup a documents contents
     */
    @Test
    public void testDocumentLookup() {
		StorageInterface database = StorageFactory.getDatabaseInstance(testPath);
    	
    	database.addDocument("http://google.com", "HELLO", "text/html", true);  
    	database.addDocument("http://google.com", "HELLO", "text/html", true);  

        assertEquals("HELLO", database.getDocument("http://google.com"));
        
		database.clearEntries();
		database.close();
    }
}
