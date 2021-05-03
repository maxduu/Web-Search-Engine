package edu.upenn.cis.cis455.storage;

import java.sql.SQLException;

public interface MasterStorageInterface {

    /**
     * How many documents so far?
     * @throws SQLException 
     */
    public int getCorpusSize() throws SQLException;

    public boolean addDocumentHash(String documentHash);

    /**
     * Shuts down / flushes / closes the storage system
     */
    public void close();

}
