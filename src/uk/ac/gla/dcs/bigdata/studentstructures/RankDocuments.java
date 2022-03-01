package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class RankDocuments implements Comparable<RankDocuments> {
	String query;
	NewsArticle doc;
	double dhpscore;


	
	
	public RankDocuments() {
		
	}
	


	public String getQuery() {
		return query;
	}



	public void setQuery(String query) {
		this.query = query;
	}



	public RankDocuments(String query, NewsArticle doc, double dhpscore) {
		super();
		this.query = query;
		this.doc = doc;
		this.dhpscore = dhpscore;
	}



	public NewsArticle getDoc() {
		return doc;
	}
	public void setDoc(NewsArticle doc) {
		this.doc = doc;
	}
	
	
	public double getDhpscore() {
		return dhpscore;
	}
	public void setDhpscore(double dhpscore) {
		this.dhpscore = dhpscore;
	}
	@Override
	public int compareTo(RankDocuments o) {
		// TODO Auto-generated method stub
		return Double.compare(this.dhpscore, o.getDhpscore());
	}
	
	

}
