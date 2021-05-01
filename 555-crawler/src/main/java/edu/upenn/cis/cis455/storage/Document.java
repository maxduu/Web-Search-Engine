package edu.upenn.cis.cis455.storage;

import java.util.Date;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * Document class
 * @author Kevin Chen
 *
 */

public class Document {
    String url;  
    String content;
    String type;
    int id;

	public Document(String url, String content, String type) {
		this.url = url;
		this.content = content;
		this.type = type;
	}
	
	public Document(int id, String url, String content, String type) {
		this.id = id;
		this.url = url;
		this.content = content;
		this.type = type;
	}
	
	public int getId() {
		return id;
	}
	
	public String getUrl() {
		return url;
	}
	
	public String getContent() {
		return content;
	}
	
	public String getType() {
		return type;
	}
}
