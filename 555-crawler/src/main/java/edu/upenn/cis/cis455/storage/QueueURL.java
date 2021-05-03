package edu.upenn.cis.cis455.storage;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

@Entity
public class QueueURL {

	@PrimaryKey
	public String url;
	
	@SecondaryKey(relate = Relationship.MANY_TO_ONE)
	public long domainId;
	
	public String toString() {
        StringBuffer buffer = new StringBuffer("QueueURL[");
        buffer.append("url=")
                .append(url)
                .append(",domainId=")
                .append(domainId)
                .append("]");
        return buffer.toString();
    }

}
