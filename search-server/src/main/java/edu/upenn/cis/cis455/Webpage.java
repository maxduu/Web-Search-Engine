package edu.upenn.cis.cis455;

class Webpage implements Comparable<Webpage>{
	
	private String url;
	private String title;
	private String preview;
	private String headers;
	private double score;
	
	public Webpage(String s, double score) {
		url = s;
		this.title = "";
		this.preview = "";
		this.headers = "";
		this.score = score;
	}
	
	public Webpage(String s, String title, String preview, String headers, double score) {
		url = s;
		this.title = title;
		this.preview = preview;
		this.headers = headers;
		this.score = score;
	}

	public String getUrl() {
		return url;
	}
	
	public void setUrl(String url) {
		this.url = url;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getPreview() {
		return preview;
	}

	public void setPreview(String preview) {
		this.preview = preview;
	}

	public String getHeaders() {
		return headers;
	}
	
	public void setHeaders(String headers) {
		this.headers = headers;
	}
	
	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}
	
	public void addToScore(double incr) {
		this.score += incr;
	}

	@Override 
	public boolean equals(Object other) {
	    if (!(other instanceof Webpage)) {
	    	return false;
	    }
	    Webpage otherWebpage = (Webpage) other;
	    return !(this.getTitle().isEmpty()) && 
	    		!(this.getPreview().isEmpty()) && 
	    		this.getTitle().equals(otherWebpage.getTitle()) && 
	    		this.getPreview().equals(otherWebpage.getPreview());
  }
	
	@Override
	public int compareTo(Webpage other) {
		// Ensure duplicate articles do not appear in resulting set
		if (this.equals(other)) {
			return 0;
		}
		
		// Webpages with higher scores are ranked higher
		// Note that we do not return 0 here since we allow for duplicate scores
		return (this.getScore() - other.getScore() < 0) ? 1 : -1;
	}
	
}
