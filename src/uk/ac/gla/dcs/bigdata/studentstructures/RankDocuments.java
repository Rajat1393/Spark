package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class RankDocuments implements Comparable<RankDocuments> {
	String query;
	NewsArticle docid;
	double dhpscore;
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}

	
	@Override
	public String toString() {
		return "RankDocuments [query=" + query + ", docid=" + docid + ", dhpscore=" + dhpscore + "]";
	}
	public RankDocuments() {
		
	}
	

	public RankDocuments(String query, NewsArticle docid, double dhpscore) {
		super();
		this.query = query;
		this.docid = docid;
		this.dhpscore = dhpscore;
	}
	public NewsArticle getDocid() {
		return docid;
	}
	public void setDocid(NewsArticle docid) {
		this.docid = docid;
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
		return Double.compare(dhpscore, o.getDhpscore());
	}
	
	

}
