package com.corp.tsdb.spark;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkPurchaseAnalysis implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2903807231514308525L;
	private transient SparkConf conf;
	private transient JavaSparkContext sc;
	private JavaRDD<String> lines;

	private JavaPairRDD<String, Tuple2<Integer, Double>> info;
	private JavaPairRDD<String, Tuple2<Integer, Double>> dataSumRangeByDay;
	private JavaPairRDD<String, Tuple2<Integer, Double>> dataSumRangeBySeason;
	private JavaPairRDD<String, Tuple2<Integer, Double>> dataSumRangeByYear;

	private String APP_NAME;
	private String MASTER_PATH;
	private String JAR_PATH;
	private String FILE_PATH;

	private boolean isYearCached;
	private boolean isSeasonCached;
	private boolean isDayCached;

	public SparkPurchaseAnalysis(String app_name, String master_path,
			String jar_path, String file_path) {
		isDayCached = false;
		isSeasonCached = false;
		isYearCached = false;
		this.APP_NAME = app_name;
		this.MASTER_PATH = master_path;
		this.JAR_PATH = jar_path;
		this.FILE_PATH = file_path;
		conf = new SparkConf()
				.setAppName(APP_NAME)
				.setMaster(MASTER_PATH)
				.setJars(
						new String[] { JAR_PATH })
				.set("spark.serializer",
						"org.apache.spark.serializer.KryoSerializer");
		sc = new JavaSparkContext(conf);
		lines = sc.textFile(FILE_PATH);
	}

	public List<Tuple2<String, Tuple2<Integer, Double>>> AnalyzeRangeByYear() {
		if (isYearCached) {
			return dataSumRangeByYear.collect();
		}
		AnalyzeRangeBySeason();
		JavaPairRDD<String, Tuple2<Integer, Double>> yearInfo = dataSumRangeBySeason
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Double>>, String, Tuple2<Integer, Double>>() {

					@Override
					public Tuple2<String, Tuple2<Integer, Double>> call(
							Tuple2<String, Tuple2<Integer, Double>> t)
							throws Exception {
						// TODO Auto-generated method stub
						String values[] = t._1.split("-");
						String year = values[0];

						return new Tuple2<String, Tuple2<Integer, Double>>(
								year, t._2);
					}
				});

		dataSumRangeByYear = yearInfo
				.reduceByKey(new Function2<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {

					@Override
					public Tuple2<Integer, Double> call(
							Tuple2<Integer, Double> v1,
							Tuple2<Integer, Double> v2) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<Integer, Double>(v1._1 + v2._1, v1._2
								+ v2._2);
					}
				});
		isYearCached = true;
		return dataSumRangeByYear.collect();
	}

	public List<Tuple2<String, Tuple2<Integer, Double>>> AnalyzeRangeBySeason() {
		if (isSeasonCached) {
			return dataSumRangeBySeason.collect();
		}
		AnalyzeRangeByDay();

		JavaPairRDD<String, Tuple2<Integer, Double>> seasonInfo = dataSumRangeByDay
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Double>>, String, Tuple2<Integer, Double>>() {

					@Override
					public Tuple2<String, Tuple2<Integer, Double>> call(
							Tuple2<String, Tuple2<Integer, Double>> t)
							throws Exception {
						// TODO Auto-generated method stub
						String values[] = t._1.split("-");
						String season = values[0] + "-"
								+ ((Integer.parseInt(values[1]) - 1) / 3 + 1);

						return new Tuple2<String, Tuple2<Integer, Double>>(
								season, t._2);
					}
				});

		dataSumRangeBySeason = seasonInfo
				.reduceByKey(new Function2<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {

					@Override
					public Tuple2<Integer, Double> call(
							Tuple2<Integer, Double> v1,
							Tuple2<Integer, Double> v2) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<Integer, Double>(v1._1 + v2._1, v1._2
								+ v2._2);
					}
				});
		isSeasonCached = true;
		return dataSumRangeBySeason.collect();
	}

	public List<Tuple2<String, Tuple2<Integer, Double>>> AnalyzeRangeByDay() {
		if (isDayCached) {
			return dataSumRangeByDay.collect();
		}
		info = lines
				.mapToPair(new PairFunction<String, String, Tuple2<Integer, Double>>() {

					private static final long serialVersionUID = -577960012111418460L;

					@Override
					public Tuple2<String, Tuple2<Integer, Double>> call(
							String str) throws Exception {
						// TODO Auto-generated method stub
						String values[] = str.split(",");
						String day = values[11].split(" ")[0];
						Tuple2<Integer, Double> valueTuple2 = new Tuple2<Integer, Double>(
								1, Double.parseDouble(values[5]));
						return new Tuple2<String, Tuple2<Integer, Double>>(day,
								valueTuple2);
					}
				});

		dataSumRangeByDay = info
				.reduceByKey(new Function2<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {
					private static final long serialVersionUID = 4052852602929190482L;

					@Override
					public Tuple2<Integer, Double> call(
							Tuple2<Integer, Double> v1,
							Tuple2<Integer, Double> v2) throws Exception {
						return new Tuple2<Integer, Double>(v1._1 + v2._1, v1._2
								+ v2._2);
					}
				});
		isDayCached = true;
		return dataSumRangeByDay.collect();
	}

	public void shutdown() {
		sc.stop();
	}

	public static void main(String[] args) {
		SparkPurchaseAnalysis analysis = new SparkPurchaseAnalysis(
				"Spark Test",
				"spark://demo2:7077",
				"/Users/xuyi/Documents/Eclipse_WorkSpace/spark-hadoop-tool/target/spark-hadoop-tool.jar",
				"hdfs://demo2:9000/user/ubuntu/PurchaseSmall.csv");
		// SparkPurchaseAnalysis analysis = SparkPurchaseAnalysis.getInstance();
		System.out.println(analysis.AnalyzeRangeByDay());
		System.out.println(analysis.AnalyzeRangeBySeason());
		System.out.println(analysis.AnalyzeRangeByYear());
		analysis.shutdown();
	}
}
