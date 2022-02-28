package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.function.MapFunction;

import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.TermDocument;

public class TransformTuples implements MapFunction<TermDocument,Tuple3<String,NewsArticle,Double>> {

	@Override
	public Tuple3<String, NewsArticle, Double> call(TermDocument t) throws Exception {

		return new Tuple3<String, NewsArticle, Double>(t.getTerm(),t.getDocument(),t.getDphScore());
	}

}
