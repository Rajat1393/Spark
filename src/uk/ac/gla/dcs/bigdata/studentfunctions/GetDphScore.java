package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.TermCorpus;
import uk.ac.gla.dcs.bigdata.studentstructures.TermDocument;

public class GetDphScore implements FlatMapFunction<TermDocument, TermDocument>{
	Broadcast<Long> broadcastDocumentLengthCorpus;
	Broadcast<Long> broadcastDocNumbers;
	Broadcast<Double> broadcastAverageLength;
	Broadcast<Map<String, Integer>> termMap;
	
	public GetDphScore(Broadcast<Long> broadcastDocumentLengthCorpus, Broadcast<Long> broadcastDocNumbers,
			Broadcast<Double> broadcastAverageLength,Broadcast<Map<String, Integer>> broadcasttermlist) {
		super();
		this.broadcastDocumentLengthCorpus = broadcastDocumentLengthCorpus;
		this.broadcastDocNumbers = broadcastDocNumbers;
		this.broadcastAverageLength = broadcastAverageLength;
		this.termMap=broadcasttermlist;
	}
	
	@Override
	public Iterator<TermDocument> call(TermDocument t) throws Exception {{
		
		Map<String,Integer> map = termMap.getValue();
		int val = map.get(t.getTerm());	
		List<TermDocument> termDocumentList  = new ArrayList<TermDocument>(1); 
		t.setDphScore(DPHScorer.getDPHScore((short) t.getCount(),val,t.getCurrentDocumentLength(), broadcastAverageLength.getValue(), broadcastDocNumbers.getValue()));
		termDocumentList.add(t);
		return termDocumentList.iterator();
	}}


}
