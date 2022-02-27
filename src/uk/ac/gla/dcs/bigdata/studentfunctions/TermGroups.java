package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.studentstructures.TermCorpus;
import uk.ac.gla.dcs.bigdata.studentstructures.TermDocument;

public class TermGroups implements FlatMapGroupsFunction<String,TermDocument,TermCorpus>{
	



/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

@Override
public Iterator<TermCorpus> call(String key, Iterator<TermDocument> values) throws Exception {
	List<TermCorpus> termbycount = new ArrayList<TermCorpus>();
	int count=0;
	while (values.hasNext()) {
		TermDocument game = values.next();
		count=count+game.getCount();
	}
	TermCorpus termo= new TermCorpus();
	termo.setCountCorpus(count);
	termo.setTerm(key);
	termbycount.add(termo);
	
			
			
	return termbycount.iterator();
}

}
