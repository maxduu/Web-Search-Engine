package edu.upenn.cis.cis455.query;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.tartarus.snowball.ext.englishStemmer;

import scala.Tuple2;

public class QueryTest {

	static final String DB_NAME = "postgres";
	static final String USERNAME = "master";
	static final String PASSWORD = "ilovezackives";
	static final int PORT = 5432;
	static final String HOSTNAME = "cis555-project.ckm3s06jrxk1.us-east-1.rds.amazonaws.com";
	
	static final String INVERTED_INDEX_TABLE_NAME = "inverted_index";
	static final String IDFS_TABLE_NAME = "idfs";
	
	static final int MAX_RESULTS = 100;
	static final englishStemmer stemmer = new englishStemmer();
	
	public static void main(String[] args) {
		if (args.length == 0) {
			System.err.println("Please enter a search query.");
			System.exit(0);
		}
		
		query(args);
	}
	
	public static List<Tuple2<Integer, Double>> query(String[] args) {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		SparkSession spark = SparkSession
				.builder()
				.appName("Query")
				.master("local[5]")
				.getOrCreate();
		
		String jdbcUrl = "jdbc:postgresql://" + HOSTNAME + ":" + PORT + "/" + 
				DB_NAME + "?user=" + USERNAME + "&password=" + PASSWORD;
		
		List<String> fullArgs = new LinkedList<>();
		for (String arg : args) {
			fullArgs.addAll(Arrays.asList(arg.split("[\\p{Punct}\\s]+")));
		}
		
		// Helper variables to map terms to indices, which we use for our vectors
		int numDistinctSearchTerms = 0;
		Map<String, Integer> termToIndex = new HashMap<>();
		
		// Helper variables to normalize term frequencies for the query vectors
		int maxCount = 0;
		Map<String, Integer> termToCount = new HashMap<>();
		
		// Construct query string for SQL
		String termsString = "(";
		for (String arg : fullArgs) {
			String search = arg.trim()
					.toLowerCase()
					.replaceFirst("^[^a-z0-9]+", "");
			if (!search.isEmpty()) {
				stemmer.setCurrent(search);
				if (stemmer.stem()) {
					search = stemmer.getCurrent();
				}
				termsString += "'" + search + "'" + ",";
				int count = termToCount.containsKey(search) ? termToCount.get(search) + 1 : 1;
				termToCount.put(search, count);
				if (count > maxCount) {
					maxCount = count;
				}
				// Map each distinct term to an index
				if (!termToIndex.containsKey(search)) {
					termToIndex.put(search, numDistinctSearchTerms);
					numDistinctSearchTerms++;
				}
			}
		}
		termsString = termsString.substring(0, termsString.length() - 1) + ")";
		
		Map<String, Double> termToQueryFreq = new HashMap<>();
		for (String key : termToCount.keySet()) {
			termToQueryFreq.put(key, ((double) termToCount.get(key)) / maxCount);
		}
		
		// Perform database queries
		System.out.println("SELECT * FROM " + INVERTED_INDEX_TABLE_NAME + " WHERE term IN " + termsString);
		
		Dataset<Row> invertedIndexDF = spark.read()
				.format("jdbc")
				.option("url", jdbcUrl)
				.option("query", "SELECT * FROM " + INVERTED_INDEX_TABLE_NAME + " WHERE term IN " + termsString)
				.load();
		
		System.out.println("SELECT * FROM " + IDFS_TABLE_NAME + " WHERE term IN " + termsString);
		
		Dataset<Row> idfsDF = spark.read()
				.format("jdbc")
				.option("url", jdbcUrl)
				.option("query", "SELECT * FROM " + IDFS_TABLE_NAME + " WHERE term IN " + termsString)
				.load();

		JavaRDD<Row> invertedIndexRDD = invertedIndexDF.toJavaRDD();
		JavaRDD<Row> idfsRDD = idfsDF.toJavaRDD();

		// Calculate weights for each query term using IDF
		JavaPairRDD<Integer, Double> queryWeights = idfsRDD.mapToPair(row -> {
			String term = row.getAs("term");
			double weight = .4 + (1 - .4) * termToQueryFreq.get(term) * (double) row.getAs("idf");
			return new Tuple2<>(termToIndex.get(term), weight);
		});
		
		// Construct weight vector for query
		final double[] queryVector = new double[numDistinctSearchTerms];
		for (Tuple2<Integer, Double> tup : queryWeights.collect()) {
			queryVector[tup._1.intValue()] = tup._2.doubleValue();
		}
		
		// Group the terms/weights corresponding to each document
		JavaPairRDD<Integer, Iterable<Tuple2<Integer, Double>>> docWeights = 
				invertedIndexRDD
				.mapToPair(row -> new Tuple2<>((int) row.getAs("id"), new Tuple2<>(termToIndex.get(row.getAs("term")), (double) row.getAs("weight"))))
				.groupByKey();

		JavaPairRDD<Integer, Double> sortedDocs = docWeights.mapToPair(pair -> {
			// Construct weight vector for each document
			double[] docVector = new double[queryVector.length];
			for (Tuple2<Integer, Double> tup : pair._2) {
				docVector[tup._1.intValue()] = tup._2.doubleValue();
			}
			return new Tuple2<>(cosineSimilarity(queryVector, docVector), pair._1);
		}).sortByKey(false).mapToPair(pair -> pair.swap());
		
		// Collect the top results
		List<Tuple2<Integer, Double>> sortedDocList = sortedDocs.take(MAX_RESULTS);
		
		for (Tuple2<Integer, Double> tup : sortedDocList) {
			System.out.println(tup._1 + " " + tup._2);
		}
		
		spark.close();

		return sortedDocList;
	}
	
	/**
	 * @return the cosine similarity between two vectors
	 */
	public static double cosineSimilarity(double[] vectorA, double[] vectorB) {
	    double dotProduct = 0.0;
	    double normA = 0.0;
	    double normB = 0.0;
	    for (int i = 0; i < vectorA.length; i++) {
	        dotProduct += vectorA[i] * vectorB[i];
	        normA += Math.pow(vectorA[i], 2);
	        normB += Math.pow(vectorB[i], 2);
	    }   
	    return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
	}

}
