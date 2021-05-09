package edu.upenn.cis.cis455;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.tartarus.snowball.ext.englishStemmer;

import scala.Tuple2;

public class Query {

	static final String DB_NAME = "postgres";
	static final String USERNAME = "master";
	static final String PASSWORD = "ilovezackives";
	static final int PORT = 5432;
	static final String HOSTNAME = "cis555-project.ckm3s06jrxk1.us-east-1.rds.amazonaws.com";
	
	static final String INVERTED_INDEX_TABLE_NAME = "inverted_index";
	static final String IDFS_TABLE_NAME = "idfs";
	
	static final String URL_TABLE_NAME = "urls";
	static final String CONTENT_TABLE_NAME = "crawler_content";
	
	static final String PAGERANK_RESULTS_TABLE_NAME = "pagerank_results";
	
	static final int MAX_RESULTS = 1000;
	
	static final englishStemmer stemmer = new englishStemmer();
	
	static final double pagerankFactor = 0.8;
	
	/* BONUSES:
	 * Title contains a search term or stemmed search term
	 * Title contains full search query
	 */
	static final double titleTermMatchBonus = 0.1;
	static final double stemmedTitleTermMatchBonus = 0.06;
	static final double titleContainsQueryBonus = 0.3;

	public static Webpage[] query(String query, SparkSession spark, Connection connect) {
		
		System.out.println("Processing query: " + query);
		
		query = query.toLowerCase();
		
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		String jdbcUrl = "jdbc:postgresql://" + HOSTNAME + ":" + PORT + "/" + 
				DB_NAME + "?user=" + USERNAME + "&password=" + PASSWORD;
		
		String[] inputArgs = query.split(" ");
		
		List<String> fullArgs = new LinkedList<>();
		for (String arg : inputArgs) {
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
				termsString += "'" + search + "',";
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
		
		// Construct weight vector for query
		final double[] queryVector = new double[numDistinctSearchTerms];
		
		Statement s;
		try {
			s = connect.createStatement(0, 0);
			System.out.println("SELECT * FROM " + IDFS_TABLE_NAME + " WHERE term IN " + termsString);
			ResultSet idfsResults = s.executeQuery("SELECT * FROM " + IDFS_TABLE_NAME + " WHERE term IN " + termsString);
			
			if (!idfsResults.next()) {
				System.out.println("No matching documents found.\n");
	    		return new Webpage[0];
			} else {
				do {
					String term = idfsResults.getString("term");
					double weight = .5 + .5 * termToQueryFreq.get(term) * idfsResults.getDouble("idf");
					queryVector[termToIndex.get(term)] = weight;
				} while (idfsResults.next());
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return new Webpage[0];
		}
		
		// Perform database query for matching documents
		System.out.println("SELECT * FROM " + INVERTED_INDEX_TABLE_NAME + " WHERE term IN " + termsString);
		
		Dataset<Row> invertedIndexDF = spark.read()
				.format("jdbc")
				.option("url", jdbcUrl)
				.option("driver", "org.postgresql.Driver")
				.option("query", "SELECT * FROM " + INVERTED_INDEX_TABLE_NAME + " WHERE term IN " + termsString)
				.load();
				
		JavaRDD<Row> invertedIndexRDD = invertedIndexDF.toJavaRDD();
		
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
		

		System.out.println("TF/IDF finished, using top " + Math.max(sortedDocList.size(), MAX_RESULTS) + " documents");
		

		/**
		 * Obtain urls, content previews for top ranked documents,
		 * and improve search results with additional measures
		 */
    	Map<Integer, String> urlMap = new HashMap<>();
    	Map<Integer, String[]> contentsMap = new HashMap<>();
    	Map<Integer, String> idToDomain = new HashMap<>();
    	Map<String, Double> domainToPagerank = new HashMap<>();
    	
    	StringBuilder idQueryBuilder = new StringBuilder("(");
    	
    	for (Tuple2<Integer, Double> tuple : sortedDocList) {
    		idQueryBuilder.append("'" + tuple._1 + "',");
    	}
    	
    	String idQueryString = idQueryBuilder.toString();
    	if (idQueryString.length() > 1) idQueryString = idQueryString.substring(0, idQueryString.length() - 1);
    	idQueryString += ")";
    	
    	String urlQuery =  String.format("SELECT * FROM %s WHERE %s IN %s", URL_TABLE_NAME, "id", idQueryString);
    	String contentQuery = String.format("SELECT * FROM %s WHERE %s IN %s", CONTENT_TABLE_NAME, "id", idQueryString);
    	
    	System.out.println(urlQuery);
    	System.out.println(contentQuery);
    	
    	try {
			ResultSet urlResults = s.executeQuery(urlQuery);
			String link = null;
			while (urlResults.next()) {
				int id = Integer.parseInt(urlResults.getString(1));
				link = urlResults.getString(2);
				urlMap.put(id,  link);
				idToDomain.put(id, new URL(link).getAuthority());
		    }
			ResultSet contentResults = s.executeQuery(contentQuery);
			while (contentResults.next()) {
				String[] add = new String[3];
				add[0] = contentResults.getString(2);
				add[1] = contentResults.getString(3);
				add[2] = contentResults.getString(4);
				contentsMap.put(Integer.parseInt(contentResults.getString(1)), add);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return new Webpage[0];
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return new Webpage[0];
		}
    	
    	// Calculate PageRank for each domain
    	StringBuilder pagerankQueryBuilder = new StringBuilder("(");
    	for (String domain : idToDomain.values()) {
    		pagerankQueryBuilder.append("'" + domain + "',");
    	}
    	String pagerankQueryString = pagerankQueryBuilder.toString();
    	if (pagerankQueryString.length() > 1) pagerankQueryString = pagerankQueryString.substring(0, pagerankQueryString.length() - 1);
    	pagerankQueryString += ")";
    	
    	String pagerankQuery =  String.format("SELECT * FROM %s WHERE %s IN %s", PAGERANK_RESULTS_TABLE_NAME, "domain", pagerankQueryString);
    	
    	System.out.println(pagerankQuery);
    	
    	try {
			ResultSet pagerankResults = s.executeQuery(pagerankQuery);
			while (pagerankResults.next()) {
				domainToPagerank.put(pagerankResults.getString("domain"), pagerankResults.getDouble("rank"));
		    }
        	s.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return new Webpage[0];
		}
    	
    	SortedSet<Webpage> webpages = new TreeSet<>(); 
    	for (Tuple2<Integer, Double> tuple : sortedDocList) {
    		int id = tuple._1;
    		String[] contents = contentsMap.get(id);
    		if (contents == null) {
    			Webpage webpage = new Webpage(urlMap.get(id), tuple._2);
    			webpage.setTitle(String.valueOf(tuple._2));
    			webpages.add(webpage);
    		} else {
    			try {
	    			Double pagerank = domainToPagerank.get(idToDomain.get(id));
	    			if (pagerank == null) {
	    				pagerank = Double.valueOf(0);
	    			}
	    			double weight = pagerankFactor * pagerank + (1 - pagerankFactor) * tuple._2;
	    			// System.out.println(urlMap.get(id) + " " + weight + " " + idToDomain.get(id) + " " + pagerank);
	    			// Webpage webpage = new Webpage(urlMap.get(id), contents[0], contents[1], contents[2], weight);
	    			
	    			Webpage webpage = new Webpage(urlMap.get(id), contents[0], contents[1], contents[2], weight);
	    			bonusWebpage(webpage, query, fullArgs);
	    			webpages.add(webpage);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}
    	}
    	
    	System.out.println("Returning final document order.\n");
    	
    	return webpages.toArray(new Webpage[0]);
	}

	/**
	 * @return the cosine similarity between two vectors
	 */
	private static double cosineSimilarity(double[] vectorA, double[] vectorB) {
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
	
	
	/* BONUSES:
	 * Title contains a search term or stemmed search term
	 * Title contains full search query
	 */
	private static void bonusWebpage(Webpage webpage, String rawQuery, List<String> queryTerms) {
		if (!webpage.getTitle().isEmpty()) {
			String title = webpage.getTitle().toLowerCase();
			Set<String> titleTerms = new HashSet<>(Arrays.asList(title.split("[\\p{Punct}\\s]+")));
			Set<String> stemmedTitleTerms = new HashSet<>();
			for (String titleTerm : titleTerms) {
				stemmer.setCurrent(titleTerm);
				if (stemmer.stem()) {
					stemmedTitleTerms.add(stemmer.getCurrent());
				} else {
					stemmedTitleTerms.add(titleTerm);
				}
			}
			
			for (String searchTerm : queryTerms) {
				if (titleTerms.contains(searchTerm.toLowerCase())) {
					webpage.addToScore(titleTermMatchBonus);
				} else {
					stemmer.setCurrent(searchTerm);
					if (stemmer.stem()) {
						if (stemmedTitleTerms.contains(stemmer.getCurrent())) {
							webpage.addToScore(stemmedTitleTermMatchBonus);
		    			}
					}
				}
			}
			if (title.contains(rawQuery.toLowerCase())) {
				webpage.addToScore(titleContainsQueryBonus);
			}
		}
	}

}
