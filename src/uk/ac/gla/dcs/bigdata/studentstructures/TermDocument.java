package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class TermDocument {
	public TermDocument(String term, NewsArticle document, int count) {
		super();
		this.term = term;
		this.document = document;
		this.count = count;
	}

	public TermDocument() {
		// TODO Auto-generated constructor stub
	}

	String term;
	NewsArticle document;
	int currentDocumentLength;
	double dphScore;
	
	public int getCurrentDocumentLength() {
		return currentDocumentLength;
	}

	public void setCurrentDocumentLength(int currentDocumentLength) {
		this.currentDocumentLength = currentDocumentLength;
	}

	public NewsArticle getDocument() {
		return document;
	}

	public double getDphScore() {
		return dphScore;
	}

	public void setDphScore(double dphScore) {
		this.dphScore = dphScore;
	}

	public void setDocument(NewsArticle document) {
		this.document = document;
	}

	int count;

	public String getTerm() {
		return term;
	}

	public void setTerm(String term) {
		this.term = term;
	}



	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
}
