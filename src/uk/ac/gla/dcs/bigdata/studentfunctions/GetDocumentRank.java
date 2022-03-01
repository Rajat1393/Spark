package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.RankDocuments;
import uk.ac.gla.dcs.bigdata.studentstructures.TermCorpus;
import uk.ac.gla.dcs.bigdata.studentstructures.TermDocument;

public class GetDocumentRank implements FlatMapFunction<Query, RankDocuments> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Broadcast<List<Tuple3<String, NewsArticle, Double>>> termdoclist;

	public GetDocumentRank(Broadcast<List<Tuple3<String, NewsArticle, Double>>> termdoclist) {
		super();
		this.termdoclist = termdoclist;
	}

	@Override
	public Iterator<RankDocuments> call(Query t) throws Exception {
		List<Tuple3<String, NewsArticle, Double>> termdoc = new ArrayList<Tuple3<String, NewsArticle, Double>>();
		termdoc = termdoclist.getValue();
		List<RankDocuments> rankeddocuments = new ArrayList<RankDocuments>();
		List<String> terms = t.getQueryTerms();
		RankDocuments r9 = new RankDocuments();
		for (String y : terms) {

			for (Tuple3<String, NewsArticle, Double> x : termdoc) {

				if (y.equals(x._1())) {

					boolean docExists = rankeddocuments.stream()
							.anyMatch(item -> x._2().getId().equals(item.getDoc().getId()));
					if (docExists) {
						r9 = rankeddocuments.stream().filter(doc -> x._2().getId().equals(doc.getDoc().getId()))
								.findAny().orElse(null);
						int index = rankeddocuments.indexOf(r9);
						if (r9 != null) {
							r9.setDhpscore(r9.getDhpscore() + x._3());
							rankeddocuments.set(index, r9);
						}
					} else {
						r9 = new RankDocuments(t.getOriginalQuery(), x._2(), x._3(),t);
						rankeddocuments.add(r9);
					}

				}
			}
		}

		return rankeddocuments.iterator();
	}

}
