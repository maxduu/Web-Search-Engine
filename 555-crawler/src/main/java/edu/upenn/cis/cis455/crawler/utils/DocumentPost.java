package edu.upenn.cis.cis455.crawler.utils;

public class DocumentPost {
	public String url;
	public String contents;
	public String type;
	public boolean modified; 

	public DocumentPost() {}
	
	public DocumentPost(String url, String contents, String type, boolean modified) {
		this.url = url;
		this.contents = contents;
		this.type = type;
		this.modified = modified;
	}
	
}
