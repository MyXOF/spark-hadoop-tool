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

public class SparkEventClientChannelTuneAnalysis implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4632037084674156781L;
	private boolean isDeviceOnlineCached;
	private boolean isChannleCached;

	private transient SparkConf conf;
	private transient JavaSparkContext sc;
	private JavaRDD<String> lines;
	
	private JavaPairRDD<String, Integer> deviceOnlineResult;
	private JavaPairRDD<String, Integer> channelTopResult;
	
	private String APP_NAME;
	private String MASTER_PATH;
	private String JAR_PATH;
	private String FILE_PATH;

	public SparkEventClientChannelTuneAnalysis(String app_name, String master_path,
			String jar_path, String file_path) {
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
		isChannleCached = false;
		isDeviceOnlineCached = false;
	}

	public List<Tuple2<String, Integer>> AnalyzeDeviceOnline() {
		if (isDeviceOnlineCached) {
			return deviceOnlineResult.collect();
		}

		JavaPairRDD<Tuple2<String, String>, Integer> deviceDataInfo = lines
				.mapToPair(new PairFunction<String, Tuple2<String, String>, Integer>() {
					@Override
					public Tuple2<Tuple2<String, String>, Integer> call(String t)
							throws Exception {
						String values[] = t.split(",");
						return new Tuple2<Tuple2<String, String>, Integer>(
								new Tuple2<String, String>(values[1], values[3]),
								1);
					}
				});

		JavaPairRDD<Tuple2<String, String>, Integer> deviceOnlineData = deviceDataInfo
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer v1, Integer v2)
							throws Exception {
						return 1;
					}
				});

		JavaPairRDD<String, Integer> deviceOnlineSumInfo = deviceOnlineData
				.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(
							Tuple2<Tuple2<String, String>, Integer> t)
							throws Exception {
						return new Tuple2<String, Integer>(t._1._1, 1);
					}
				});

		deviceOnlineResult = deviceOnlineSumInfo
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer v1, Integer v2)
							throws Exception {
						return v1 + v2;
					}
				});

		isDeviceOnlineCached = true;
		return deviceOnlineResult.collect();
	}

	public  List<Tuple2<String,Integer>> AnalyzeChannel() {
		if (isChannleCached) {
			return channelTopResult.collect();
		}

		JavaPairRDD<String, Integer> channleInfo = lines
				.mapToPair(new PairFunction<String, String, Integer>() {

					@Override
					public Tuple2<String, Integer> call(String t)
							throws Exception {
						String values[] = t.split(",");
						return new Tuple2<String, Integer>(values[4], 1);
					}
				});

		channelTopResult = channleInfo
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer v1, Integer v2)
							throws Exception {
						// TODO Auto-generated method stub
						return v1 + v2;
					}
				});

		isChannleCached = true;
		return channelTopResult.collect();
	}
	
	public void shutdown(){
		sc.stop();
	}
	
	public static void main(String[] args) {
		SparkEventClientChannelTuneAnalysis analysis = new SparkEventClientChannelTuneAnalysis(
				"Spark Test",
				"spark://demo2:7077",
				"/Users/xuyi/Documents/Eclipse_WorkSpace/spark-hadoop-tool/target/spark-hadoop-tool-0.0.1-SNAPSHOT.jar",
				"hdfs://demo2:9000/user/ubuntu/EventClientChannelTuneSmall.csv");
		System.out.println(analysis.AnalyzeChannel());
		System.out.println(analysis.AnalyzeDeviceOnline());
		System.out.println(analysis.AnalyzeChannel());
		System.out.println(analysis.AnalyzeDeviceOnline());
		analysis.shutdown();
	}


}
