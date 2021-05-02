package edu.upenn.cis.cis455.invertedindex;

import java.io.Serializable;

public class InvertedIndexEntry implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private int id;
	private String term;
	private double tf;
	private double weight;
	
	public int getId() {
		return id;
	}
	
	public String getTerm() {
		return term;
	}
	
	public double getTf() {
		return tf;
	}
	
	public double getWeight() {
		return weight;
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
	
	public void setWeight(double weight) {
		this.weight = weight;
	}
	
}
