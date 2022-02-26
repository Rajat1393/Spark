package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.TermCorpus;
import uk.ac.gla.dcs.bigdata.studentstructures.TermDocument;

public class GetTermCorpusCount implements FlatMapFunction<TermDocument,TermCorpus>{

	@Override
	public Iterator<TermCorpus> call(TermDocument t) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
