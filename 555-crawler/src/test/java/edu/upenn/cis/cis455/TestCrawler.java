package edu.upenn.cis.cis455;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.cis455.crawler.Crawler;
import edu.upenn.cis.cis455.storage.MasterStorageInterface;
import junit.framework.TestCase;

public class TestCrawler extends TestCase {
	
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
     * Makes sure the crawler saves documents into the database
     */
    @Test
    public void testCrawlerIndexes() {
//		StorageInterface database = StorageFactory.getDatabaseInstance(testPath);
//    	
//    	CrawlerM1 crawler = new CrawlerM1("https://crawltest.cis.upenn.edu/", database, 1, 1);
//        crawler.start();
//        
//        while (!crawler.isDone())
//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }       
//
//        assertNotNull(database.getDocumentObjectByUrl("https://crawltest.cis.upenn.edu/"));
//        
//		database.clearEntries();
//        crawler.shutdown();
    }
    
    /**
     * Makes sure the crawler terminates after the count is reached
     */
    @Test
    public void testCrawlerStopsCount() {
//		StorageInterface database = StorageFactory.getDatabaseInstance(testPath);
//    	
//    	CrawlerM1 crawler = new CrawlerM1("https://crawltest.cis.upenn.edu", database, 1, 3);
//        crawler.start();
//        
//        while (!crawler.isDone())
//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        
//		// we can allow workers to finish their current URL being handled
//		assertTrue(database.getCorpusSize() - 3 <= CrawlerM1.NUM_WORKERS);
//		
//		database.clearEntries();
//        crawler.shutdown();
    }
    
    /**
     * Make sure the crawler follows robots.txt disallow
     */
    @Test
    public void testCrawlerDisallow() {
//		StorageInterface database = StorageFactory.getDatabaseInstance(testPath);
//
//    	CrawlerM1 crawler = new CrawlerM1("https://crawltest.cis.upenn.edu/marie/private/", database, 1, 1);
//        crawler.start();
//        
//        while (!crawler.isDone())
//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        
//		assertEquals(0, database.getCorpusSize());
//		
//		database.clearEntries();
//        crawler.shutdown();
    }

}
