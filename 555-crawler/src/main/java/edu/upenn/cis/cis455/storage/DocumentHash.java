package edu.upenn.cis.cis455.storage;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * Document hash class used in BerkeleyDB for content seen
 * @author Kevin Chen
 *
 */
@Entity
public class DocumentHash {
    @PrimaryKey
    public String hash;
    
    public int documentId;

	public String toString() {
        StringBuffer buffer = new StringBuffer("DocumentHash[");
        buffer.append("hash=")
                .append(hash)
                .append("documentId=")
                .append(documentId)
                .append("]");
        return buffer.toString();
    }
}
