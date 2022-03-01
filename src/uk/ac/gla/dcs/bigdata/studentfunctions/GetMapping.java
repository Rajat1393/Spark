package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.TermDocument;



public class GetMapping implements FlatMapFunction<NewsArticle,TermDocument>{
	Broadcast<List<String>> term;
	public GetMapping(Broadcast<List<String>> term) {
		super();
		this.term = term;
	}

	@Override
	public Iterator<TermDocument> call(NewsArticle t) throws Exception {
		List<TermDocument> termDocumentList = new ArrayList<TermDocument>();
			
			List<ContentItem> contentList = t.getContents();
			
			List<String> terms = term.value();
			for (String term : terms) {
				int count = 0;
			TermDocument termDocument = new TermDocument();
			int documentLength = 0;
			for (ContentItem contentItem : contentList) {
				documentLength = documentLength + contentItem.getContent().length();
				 count = count + countOccurrences(contentItem.getContent(), term);	
			}
			
			termDocument.setCount(count);
			termDocument.setDocument(t);
			termDocument.setTerm(term);
			termDocument.setCurrentDocumentLength(documentLength);
			termDocumentList.add(termDocument);
			}
			
	
		return termDocumentList.iterator();
	}

	static int countOccurrences(String str, String word)
	{
	    // split the string by spaces in a
	    String a[] = str.split(" ");
	 
	    // search for pattern in a
	    int count = 0;
	    for (int i = 0; i < a.length; i++)
	    {
	    // if match found increase count
	    if (word.equals(a[i]))
	        count++;
	    }
	 
	    return count;
	}
}
