package edu.upenn.cis.cis455.invertedindex;

import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;

public class Example {
	public void SparkExample() {
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkFileSumApp");
	    JavaSparkContext sc = new JavaSparkContext(sparkConf);

	    JavaRDD<String> input = sc.textFile("numbers.txt");
	    JavaRDD<String> numberStrings = input.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
	    JavaRDD<String> validNumberString = numberStrings.filter(string -> !string.isEmpty());
	    JavaRDD<Integer> numbers = validNumberString.map(numberString -> Integer.valueOf(numberString));
	    int finalSum = numbers.reduce((x,y) -> x+y);

	    System.out.println("Final sum is: " + finalSum);

	    sc.close();
	}
}
