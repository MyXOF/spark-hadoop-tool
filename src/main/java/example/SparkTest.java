package example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class SparkTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf()
				.setAppName("Spark Test")
				.setMaster("spark://demo2:7077")
				.setJars(new String[] { "/Users/xuyi/Documents/Eclipse_WorkSpace/spark-hadoop-tool/target/spark-hadoop-tool-0.0.1-SNAPSHOT.jar" });
		JavaSparkContext sc = new JavaSparkContext(conf);
		// JavaRDD<String> lines = sc.textFile("Device-2.csv");
		JavaRDD<String> lines = sc
				.textFile("hdfs://demo2:9000/user/ubuntu/README.md");
		// JavaRDD<String> lines =
		// sc.textFile("hdfs://demo2:9000/user/ubuntu/EventClientMenu.csv");

		JavaRDD<Integer> lineLengths = lines
				.map(new Function<String, Integer>() {
					@Override
					public Integer call(String s) {
						return s.length();
					}
				});

		int totalLength = lineLengths
				.reduce(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer a, Integer b) {
						return a + b;
					}
				});

		System.out.println("sum is: " + totalLength);
		sc.stop();

	}

}
