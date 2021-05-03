package edu.upenn.cis.cis455.storage;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class Domain {

	@PrimaryKey
	public String domain;
	public int id;
	
	public String toString() {
        StringBuffer buffer = new StringBuffer("Domain[");
        buffer.append("domain=")
                .append(domain)
                .append(",id=")
                .append(id)
                .append("]");
        return buffer.toString();
    }
	
}
