package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;


import uk.ac.gla.dcs.bigdata.studentstructures.TermCorpus;
import uk.ac.gla.dcs.bigdata.studentstructures.TermDocument;

public class GetTermCorpusCount implements  MapFunction<TermDocument,String>{

	@Override
	public String call(TermDocument value) throws Exception {
		// TODO Auto-generated method stub
		return value.getTerm();
	}



}
