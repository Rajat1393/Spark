package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.TermDocument;

public class GetDphScore implements FlatMapFunction<TermDocument, TermDocument>{
	Broadcast<Long> broadcastDocumentLengthCorpus;
	Broadcast<Long> broadcastDocNumbers;
	Broadcast<Double> broadcastAverageLength;
	
	public GetDphScore(Broadcast<Long> broadcastDocumentLengthCorpus, Broadcast<Long> broadcastDocNumbers,
			Broadcast<Double> broadcastAverageLength) {
		super();
		this.broadcastDocumentLengthCorpus = broadcastDocumentLengthCorpus;
		this.broadcastDocNumbers = broadcastDocNumbers;
		this.broadcastAverageLength = broadcastAverageLength;
	}
	
	@Override
	public Iterator<TermDocument> call(TermDocument t) throws Exception {
		// TODO Auto-generated method stub
		List<TermDocument> termDocumentList  = new ArrayList<TermDocument>(1); 
		t.setDphScore(DPHScorer.getDPHScore((short) t.getCount(), 20, t.getCurrentDocumentLength(), broadcastAverageLength.getValue(), broadcastDocNumbers.getValue()));
		termDocumentList.add(t);
		return termDocumentList.iterator();
	}


}
