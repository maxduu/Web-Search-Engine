package edu.upenn.cis.cis455.invertedindex;

import java.io.Serializable;

public class IDFEntry implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String term;
	private double idf;
	
	public String getTerm() {
		return term;
	}
	
	public double getIdf() {
		return idf;
	}
	
	public void setTerm(String term) {
		this.term = term;
	}
	
	public void setIdf(double idf) {
		this.idf = idf;
	}
}
