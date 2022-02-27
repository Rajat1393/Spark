package uk.ac.gla.dcs.bigdata.studentstructures;

public class TermCorpus {
String term;
int countCorpus;
public String getTerm() {
	return term;
}
public void setTerm(String term) {
	this.term = term;
}
public TermCorpus(String term, int countCorpus) {
	super();
	this.term = term;
	this.countCorpus = countCorpus;
}
public TermCorpus() {
	
	// TODO Auto-generated constructor stub
}
public int getCountCorpus() {
	return countCorpus;
}
public void setCountCorpus(int countCorpus) {
	this.countCorpus = countCorpus;
}
}
