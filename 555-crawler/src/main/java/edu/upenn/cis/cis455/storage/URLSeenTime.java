package edu.upenn.cis.cis455.storage;

import java.util.Date;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * URL Seen class used in BerkeleyDB
 * @author Kevin Chen
 *
 */
@Entity
public class URLSeenTime {

    @PrimaryKey
    public String url; 
    public Date lastCrawled;

	public String toString() {
        StringBuffer buffer = new StringBuffer("URLSeenTime[");
        buffer.append("url=")
                .append(url)
                .append(",lastCrawled=")
                .append(lastCrawled)
                .append("]");
        return buffer.toString();
    }
}
