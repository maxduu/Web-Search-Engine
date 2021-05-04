package edu.upenn.cis.cis455.invertedindex;

import java.util.*;
import java.io.*;


/**
 * Stop word reader that supplies a set of English stop words
 * by reading stop words from a downloaded file
 */
public class StopWordReader {

    public Set<String> getStopWords() {
    	BufferedReader reader = null;
    	try {
    		InputStream in = getClass().getResourceAsStream("/nlp_en_stop_words.txt");
            reader = new BufferedReader(new InputStreamReader(in));
        } catch (Exception e) {
            e.printStackTrace();
        }
    	
    	String nextLine;
        Set<String> stopWords = new HashSet<String>();
        try {
            while ((nextLine = reader.readLine()) != null) {
                stopWords.add(nextLine);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    	
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return stopWords;
    }
}
