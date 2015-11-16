package example;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkTest implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -8756405274346815576L;
	private transient SparkConf conf;
	private transient JavaSparkContext sc;
	private JavaRDD<String> lines;

	public SparkTest() {
		conf = new SparkConf()
				.setAppName("Spark Test")
				.setMaster("spark://demo2:7077")
				.setJars(new String[] { "/Users/xuyi/Documents/Eclipse_WorkSpace/spark-hadoop-tool/target/spark-hadoop-tool-0.0.1-SNAPSHOT.jar" })
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sc = new JavaSparkContext(conf);
		// JavaRDD<String> lines = sc.textFile("Device-2.csv");
		lines = sc.textFile("hdfs://demo2:9000/user/ubuntu/PurchaseSmall.csv");
	}
	
	public void serivice(){
		JavaPairRDD<String,Tuple2<Integer, Double>> info = lines.mapToPair(new PairFunction<String, String,Tuple2<Integer, Double>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -577960012111418460L;

			@Override
			public Tuple2<String, Tuple2<Integer, Double>> call(String str)
					throws Exception {
				// TODO Auto-generated method stub
				String values[] = str.split(",");
				String day = values[11].split(" ")[0];
				Tuple2<Integer,Double> valueTuple2 = new Tuple2<Integer,Double>(1, Double.parseDouble(values[5]));
				return new Tuple2<String, Tuple2<Integer, Double>>(day,valueTuple2);
			}
		});
		
		JavaPairRDD<String,Tuple2<Integer, Double>> dataSumRangeByDay = info.reduceByKey(new Function2<Tuple2<Integer,Double>, Tuple2<Integer,Double>, Tuple2<Integer,Double>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 4052852602929190482L;

			@Override
			public Tuple2<Integer, Double> call(Tuple2<Integer, Double> v1,
					Tuple2<Integer, Double> v2) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Integer, Double>(v1._1+v2._1,v1._2+v2._2);
			}
		});

		List<Tuple2<String, Tuple2<Integer, Double>>> list = dataSumRangeByDay.collect();
		System.out.println(list);
		sc.stop();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		SparkConf conf = new SparkConf()
//				.setAppName("Spark Test")
//				.setMaster("spark://demo2:7077")
//				.setJars(
//						new String[] { "/Users/xuyi/Documents/Eclipse_WorkSpace/spark-hadoop-tool/target/spark-hadoop-tool-0.0.1-SNAPSHOT.jar" });
//		JavaSparkContext sc = new JavaSparkContext(conf);
//		// JavaRDD<String> lines = sc.textFile("Device-2.csv");
//		JavaRDD<String> lines = sc
//				.textFile("hdfs://demo2:9000/user/ubuntu/README.md");
//		// JavaRDD<String> lines =
//		// sc.textFile("hdfs://demo2:9000/user/ubuntu/EventClientMenu.csv");
//
//		JavaRDD<Integer> lineLengths = lines
//				.map(new Function<String, Integer>() {
//					@Override
//					public Integer call(String s) {
//						return s.length();
//					}
//				});
//
//		int totalLength = lineLengths
//				.reduce(new Function2<Integer, Integer, Integer>() {
//					@Override
//					public Integer call(Integer a, Integer b) {
//						return a + b;
//					}
//				});
//
//		System.out.println("sum is: " + totalLength);
//		sc.stop();
		SparkTest test= new SparkTest();
		test.serivice();

	}

}
