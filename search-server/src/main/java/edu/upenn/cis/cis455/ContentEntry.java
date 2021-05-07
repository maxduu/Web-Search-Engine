package edu.upenn.cis.cis455;

public class ContentEntry {
	private static final long serialVersionUID = 1L;
	
	private int id;
	private String title;
	private String content;
	private String headers;
	public ContentEntry(int id, String title, String content, String headers) {
		this.id = id;
		this.content = content;
		this.title = title;
		this.headers = headers;
	}
	
	public int getId() {
		return id;
	}
	
	public String getTitle() {
		return title;
	}
	
	public String getContent() {
		return content;
	}
	
	public String getHeaders() {
		return headers;
	}

	
	public void setId(int id) {
		this.id = id;
	}
	
	public void setTitle(String title) {
		this.title = title;
	}

	public void setContent(String content) {
		this.content = content;
	}
	
	public void setHeader(String headers) {
		this.headers = headers;
	}


}
