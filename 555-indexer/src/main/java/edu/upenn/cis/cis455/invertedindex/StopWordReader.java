package edu.upenn.cis.cis455.invertedindex;

import java.util.*;
import java.io.*;


/**
 * Stop word reader that supplies a set of English stop words
 * by reading stop words from a downloaded file
 */
public class StopWordReader {

    public static Set<String> getStopWords() {
    	BufferedReader reader = null;
    	try {
            reader = new BufferedReader(new FileReader("src/main/resources/nlp_en_stop_words.txt"));
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
