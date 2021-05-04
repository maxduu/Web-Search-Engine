package edu.upenn.cis.cis455;

public class ContentEntry {
	private static final long serialVersionUID = 1L;
	
	private int id;
	private String title;
	private String content;
	
	public ContentEntry(int id, String title, String content) {
		this.id = id;
		this.content = content;
		this.title = title;
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
	
	public void setId(int id) {
		this.id = id;
	}
	
	public void setTitle(String title) {
		this.title = title;
	}

	public void setContent(String content) {
		this.content = content;
	}

}
