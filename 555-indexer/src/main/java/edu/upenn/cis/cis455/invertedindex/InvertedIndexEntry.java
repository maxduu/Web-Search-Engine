package edu.upenn.cis.cis455.invertedindex;

import java.io.Serializable;

public class InvertedIndexEntry implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private int id;
	private String term;
	private double tf;
	
	public int getId() {
		return id;
	}
	
	public String getTerm() {
		return term;
	}
	
	public double getTf() {
		return tf;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public void setTerm(String term) {
		this.term = term;
	}
	
	public void setTf(double tf) {
		this.tf = tf;
	}
	
}
