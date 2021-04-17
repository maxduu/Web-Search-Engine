package edu.upenn.cis.cis455.storage;

import java.util.Date;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * Document class used in BerkeleyDB
 * @author Kevin Chen
 *
 */
@Entity
public class Document {
    @PrimaryKey(sequence="documentId")
    public int id;
    
    @SecondaryKey(relate = Relationship.ONE_TO_ONE)
    public String url; 
    
    public String content;
    public Date lastCrawled;
    public String type;

	public String toString() {
        StringBuffer buffer = new StringBuffer("Document[");
        buffer.append("id=")
                .append(id)
                .append(",url=")
                .append(url)
                .append(",content=")
                .append(content)
                .append(",lastCrawled=")
                .append(lastCrawled)
                .append(",type=")
                .append(type)
                .append("]");
        return buffer.toString();
    }
}
