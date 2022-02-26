package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;


public class StopWordRemovalFlatMap implements FlatMapFunction<Query,Query>{

	@Override
	public Iterator<Query> call(Query t) throws Exception {
		TextPreProcessor textprocessor = new TextPreProcessor();
		List<String> tokens= textprocessor.process(t.getOriginalQuery());
		List<Query> queryList  = new ArrayList<Query>(1); 
		short[] queryTermCounts = {(short)tokens.size()};
		Query q = new Query(t.getOriginalQuery(),tokens,queryTermCounts);
		queryList.add(q);
		return queryList.iterator();
	}

}
