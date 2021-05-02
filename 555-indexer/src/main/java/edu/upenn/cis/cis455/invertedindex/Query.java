package edu.upenn.cis.cis455.invertedindex;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.tartarus.snowball.ext.englishStemmer;

public class Query {

	static final String DB_NAME = "postgres";
	static final String USERNAME = "master";
	static final String PASSWORD = "ilovezackives";
	static final int PORT = 5432;
	static final String HOSTNAME = "cis555-project.ckm3s06jrxk1.us-east-1.rds.amazonaws.com";
	
	static final String INVERTED_INDEX_TABLE_NAME = "inverted_index";
	static final String IDFS_TABLE_NAME = "idfs";
	
	public static void main(String[] args) {
		if (args.length == 0) {
			System.err.println("Please enter a query.");
			System.exit(0);
		}
		
		query(args);
	}
	
	public static List<String> query(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("Query")
				.master("local[5]")
				.getOrCreate();
		
		String jdbcUrl = "jdbc:postgresql://" + HOSTNAME + ":" + PORT + "/" + 
				DB_NAME + "?user=" + USERNAME + "&password=" + PASSWORD;
		
		String termsString = "(";
		for (String arg : args) {
			englishStemmer stemmer = new englishStemmer();
			stemmer.setCurrent(arg);
			if (stemmer.stem()){
				arg = stemmer.getCurrent();
			}
			termsString += "'" + arg + "'" + ",";
		}
		termsString = termsString.substring(0, termsString.length() - 1) + ")";
		
		System.out.println("SELECT * FROM " + INVERTED_INDEX_TABLE_NAME + " WHERE term IN " + termsString);
		
		Dataset<Row> invertedIndexDF = spark.read()
				.format("jdbc")
				.option("url", jdbcUrl)
				.option("query", "SELECT * FROM " + INVERTED_INDEX_TABLE_NAME + " WHERE term IN " + termsString)
				.load();
		
		Dataset<Row> idfsDF = spark.read()
				.format("jdbc")
				.option("url", jdbcUrl)
				.option("query", "SELECT * FROM " + IDFS_TABLE_NAME + " WHERE term IN " + termsString)
				.load();
		
		invertedIndexDF.show();
		idfsDF.show();
		
		// Rows of Documents -> Term, TF
		
		// JavaPairRDD<<String, String>, Double> pairToTF
		// JavaPairRDD<String, Double> termToIDF
		// JavaPairRDD<String, Double> docToSimilarity
		spark.close();
		
		return null;
	}
	
	

}
