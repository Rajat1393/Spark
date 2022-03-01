package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import com.google.common.base.Predicate;

import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentfunctions.DocumentTermsProcessingFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.GetDocumentRank;
import uk.ac.gla.dcs.bigdata.studentfunctions.GetDphScore;
import uk.ac.gla.dcs.bigdata.studentfunctions.GetMapping;
import uk.ac.gla.dcs.bigdata.studentfunctions.GetTermCorpusCount;
import uk.ac.gla.dcs.bigdata.studentfunctions.StopF;
import uk.ac.gla.dcs.bigdata.studentfunctions.StopWordRemovalFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.TermGroups;
import uk.ac.gla.dcs.bigdata.studentfunctions.TransformTuples;
import uk.ac.gla.dcs.bigdata.studentstructures.RankDocuments;
import uk.ac.gla.dcs.bigdata.studentstructures.TermCorpus;
import uk.ac.gla.dcs.bigdata.studentstructures.TermDocument;
import java.util.function.Function;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by the
 * spark.master environment variable.
 * 
 * @author Richard
 *
 */
public class AssessedExercise {

	public static void main(String[] args) throws IOException {

		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get
														// an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark
																			// finds it

		// The code submitted for the assessed exerise may be run in either local or
		// remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef == null)
			sparkMasterDef = "local[2]"; // default is local mode with two executors

		String sparkSessionName = "BigDataAE"; // give the session a name

		// Create the Spark Configuration
		SparkConf conf = new SparkConf().setMaster(sparkMasterDef).setAppName(sparkSessionName);

		// Create the spark session
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");

		if (queryFile == null)
			queryFile = "data/queries.list"; // default is a sample with 3 queries

		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile == null)
			newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news
																				// articles

		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

		// Close the spark session
		spark.close();

		// Check if the code returned any results
		if (results == null)
			System.err
					.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {

			// We have set of output rankings, lets write to disk

			// Create a new folder
			File outDirectory = new File("results/" + System.currentTimeMillis());
			if (!outDirectory.exists())
				outDirectory.mkdir();

			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}

	}

	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile)
			throws IOException {

		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article

		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java
		// objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts
																										// each row into
																										// a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this
																											// converts
		// process queries
		StopWordRemovalFlatMap queryStopWordRemoval = new StopWordRemovalFlatMap();
		Dataset<Query> processedQueries = queries.flatMap(queryStopWordRemoval, Encoders.bean(Query.class));
		
		// length of corpus
		LongAccumulator documentLengthAccumulator = spark.sparkContext().longAccumulator();
		LongAccumulator totalDocsInCorpusAccumulator = spark.sparkContext().longAccumulator();

		// process documents
		DocumentTermsProcessingFlatMap documentStopWordRemoval = new DocumentTermsProcessingFlatMap(
				documentLengthAccumulator, totalDocsInCorpusAccumulator);
		Dataset<NewsArticle> processedDocuments = news.flatMap(documentStopWordRemoval,
				Encoders.bean(NewsArticle.class));
		
		processedDocuments.collectAsList();
		long documentLengthCorpus = documentLengthAccumulator.value();
		long docNumbers = totalDocsInCorpusAccumulator.value();
		double averageLength = documentLengthCorpus / docNumbers;

		StopF stop = new StopF();
		Dataset<String> p = queries.flatMap(stop, Encoders.STRING());
		List<String> terms = p.collectAsList();
		// Broadcast values

		Broadcast<List<String>> broadcastTerms = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(terms);
		Broadcast<Long> broadcastDocumentLengthCorpus = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(documentLengthCorpus);

		Broadcast<Long> broadcastDocNumbers = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(docNumbers);

		Broadcast<Double> broadcastAverageLength = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(averageLength);

		GetMapping getu = new GetMapping(broadcastTerms);
		Dataset<TermDocument> termsg = processedDocuments.flatMap(getu, Encoders.bean(TermDocument.class));
		termsg.collectAsList();

		GetTermCorpusCount keyFunction = new GetTermCorpusCount();
		KeyValueGroupedDataset<String, TermDocument> termByDoc = termsg.groupByKey(keyFunction, Encoders.STRING());

		TermGroups termgr = new TermGroups();
		Dataset<TermCorpus> termc = termByDoc.flatMapGroups(termgr, Encoders.bean(TermCorpus.class));
		List<TermCorpus> termclist = termc.collectAsList();

		Map<String, Integer> map = new HashMap<>();
		for (TermCorpus ter : termclist) {
			map.put(ter.getTerm(), ter.getCountCorpus());
		}

		Broadcast<Map<String, Integer>> broadcasttermclist = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(map);

		GetDphScore dphScore = new GetDphScore(broadcastDocumentLengthCorpus, broadcastDocNumbers,
				broadcastAverageLength, broadcasttermclist);
		Dataset<TermDocument> termsDocumentWithScore = termsg.flatMap(dphScore, Encoders.bean(TermDocument.class));
		List<TermDocument> termsgList = termsDocumentWithScore.collectAsList();

		TransformTuples tu = new TransformTuples();

		Dataset<Tuple3<String, NewsArticle, Double>> termtransform = termsDocumentWithScore.map(tu,
				Encoders.tuple(Encoders.STRING(), Encoders.bean(NewsArticle.class), Encoders.DOUBLE()));
		List<Tuple3<String, NewsArticle, Double>> termtransformlist = termtransform.collectAsList();
		Broadcast<List<Tuple3<String, NewsArticle, Double>>> broadcasttransformlist = JavaSparkContext
				.fromSparkContext(spark.sparkContext()).broadcast(termtransformlist);
		GetDocumentRank getrank = new GetDocumentRank(broadcasttransformlist);

		Dataset<RankDocuments> ranks = processedQueries.flatMap(getrank, Encoders.bean(RankDocuments.class));

		List<RankDocuments> ranksDocList = ranks.collectAsList();

		Collections.sort(ranksDocList, Collections.reverseOrder());

		List<RankDocuments> r = ranksDocList.stream().filter(c -> !Double.valueOf(c.getDhpscore()).isNaN())
				.collect(Collectors.toList());

		r.stream().forEach(k -> k.setDhpscore(divideScore(k)));
		
		Map<String, List<RankDocuments>> studlistGrouped = r.stream()
				.collect(Collectors.groupingBy(RankDocuments::getQuery));

		List<DocumentRanking> documentRankingList = new ArrayList<DocumentRanking>();
		for (String key : studlistGrouped.keySet()) {
			DocumentRanking docRank = new DocumentRanking();
			
			List<RankedResult> results = new ArrayList<RankedResult>();
			List<RankDocuments> tk = studlistGrouped.get(key);

			ArrayList<RankDocuments> a1 = new ArrayList<RankDocuments>(tk);
			ArrayList<RankDocuments> a2 = new ArrayList<RankDocuments>();
			  
			Query query = a1.get(0).getOriginalQuery();
			for (int i = 0; i < a1.size()-1; i++) {
				for (int k = i + 1; k < a1.size(); k++) {
					String doc1 = a1.get(i).getDoc().getTitle();
					String doc2 = a1.get(k).getDoc().getTitle();
					double similarity = TextDistanceCalculator.similarity(doc1, doc2);
					if (similarity < 0.5) {
						a2.add(a1.get(k));
					}
				}
			}
			a1.removeAll(a2);
			for (int i = 0; i < 10; i++) {
				RankedResult ranked = new RankedResult();
				ranked.setArticle(a1.get(i).getDoc());
				ranked.setDocid(a1.get(i).getDoc().getId());
				ranked.setScore(a1.get(i).getDhpscore());
				results.add(ranked);
			}
			docRank.setQuery(query);
			docRank.setResults(results);
			documentRankingList.add(docRank);
		}
		return documentRankingList;

	}
	
	static double divideScore(RankDocuments r) {
		String[] splitted = r.getQuery().trim().split(" ");
		int selectedItems = splitted.length;
		double k = r.getDhpscore()/selectedItems;
		return k;
	}

}
