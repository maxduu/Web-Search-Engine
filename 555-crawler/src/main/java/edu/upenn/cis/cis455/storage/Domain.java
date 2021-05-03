package edu.upenn.cis.cis455.storage;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class Domain {

	@PrimaryKey
	public long id;

	public String domain;
	public String robotsTxtContent;
	
	public String toString() {
        StringBuffer buffer = new StringBuffer("Domain[");
        buffer.append("id=")
                .append(id)
                .append(",domain=")
                .append(domain)
                .append(",robotsTxtContent=")
                .append(robotsTxtContent)
                .append("]");
        return buffer.toString();
    }
	
}
