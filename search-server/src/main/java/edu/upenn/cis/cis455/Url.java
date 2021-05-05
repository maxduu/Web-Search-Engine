package edu.upenn.cis.cis455;

class Url {
	private String url;
	private String title, content;
	public Url(String s) {
		url = s;
	}
	
	public Url(String s, String title, String content) {
		url = s;
		this.title = title;
		this.content = content;
	}

	public String getUrl() {
		return url;
	}
	
	public void setUrl(String url) {
		this.url = url;
	}
}
